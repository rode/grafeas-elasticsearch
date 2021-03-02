// Copyright 2021 The Rode Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filtering

import (
	"encoding/json"
	"fmt"
	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/parser"
	"github.com/hashicorp/go-multierror"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"regexp"
	"strings"
)

type Filterer interface {
	ParseExpression(filter string) (*Query, error)
	Parse(filter string) (interface{}, error)
}

type filterer struct{}

func NewFilterer() Filterer {
	return &filterer{}
}

var elasticsearchSpecialCharacterRegex = regexp.MustCompile(`([\-=&|!(){}\[\]^"~*?:\\/])`)

// ParseExpression will serve as the entrypoint to the filter
// that is eventually passed to parseExpression which will handle the recursive logic
func (f *filterer) ParseExpression(filter string) (*Query, error) {
	parsedExpr, commonErr := parser.Parse(common.NewStringSource(filter, ""))
	if len(commonErr.GetErrors()) > 0 {
		resultErr := fmt.Errorf("error parsing filter")
		for _, e := range commonErr.GetErrors() {
			resultErr = multierror.Append(resultErr, fmt.Errorf("%s (%d:%d)", e.Message, e.Location.Line(), e.Location.Column()))
		}

		return nil, resultErr
	}

	expression := parsedExpr.GetExpr()

	if _, ok := expression.GetExprKind().(*expr.Expr_CallExpr); !ok {
		return nil, fmt.Errorf("expected call expression when parsing filter, got %T", expression.GetExprKind())
	}

	return parseExpression(expression)
	//return f.parse(expression)
}

func (f *filterer) Parse(filter string) (interface{}, error) {
	parsedExpr, commonErr := parser.Parse(common.NewStringSource(filter, ""))
	if len(commonErr.GetErrors()) > 0 {
		resultErr := fmt.Errorf("error parsing filter")
		for _, e := range commonErr.GetErrors() {
			resultErr = multierror.Append(resultErr, fmt.Errorf("%s (%d:%d)", e.Message, e.Location.Line(), e.Location.Column()))
		}

		return nil, resultErr
	}

	expression := parsedExpr.GetExpr()

	b, _ := json.Marshal(&expression)
	fmt.Printf("%s\n", b)

	return parse(expression)
}

func twoArityArguments(functionExpr *expr.Expr_Call) (interface{}, interface{}, error) {
	if len(functionExpr.Args) != 2 {
		return nil, nil, fmt.Errorf("unexpected number of arguments: %d", len(functionExpr.Args))
	}

	leftArg := functionExpr.Args[0]
	parsedLeftArg, err := parse(leftArg)
	if err != nil {
		return nil, nil, err
	}
	rightArg := functionExpr.Args[1]
	parsedRightArg, err := parse(rightArg)
	if err != nil {
		return nil, nil, err
	}

	return parsedLeftArg, parsedRightArg, nil
}

// target, arg, error
func targetAndArgument(functionExpr *expr.Expr_Call) (interface{}, interface{}, error) {
	target := functionExpr.Target
	if len(functionExpr.Args) != 1 {
		return nil, nil, fmt.Errorf("unexpected number of arguments: %d", len(functionExpr.Args))
	}
	arg := functionExpr.Args[0] // TODO: length checks

	parsedTarget, err := parse(target)
	if err != nil {
		return nil, nil, err
	}

	parsedArg, err := parse(arg)
	if err != nil {
		return nil, nil, err
	}

	return parsedTarget, parsedArg, nil
}

func parse(expression *expr.Expr) (interface{}, error) {
	switch expression.ExprKind.(type) {
	case *expr.Expr_IdentExpr:
		return expression.GetIdentExpr().Name, nil
	case *expr.Expr_ConstExpr:
		return expression.GetConstExpr().GetStringValue(), nil // TODO: type switch for non-string literals
	case *expr.Expr_SelectExpr:
		selectExpr := expression.GetSelectExpr()

		val, err := parse(selectExpr.Operand)
		if err != nil {
			return nil, err
		}

		return fmt.Sprintf("%s.%s", val, selectExpr.Field), nil
	case *expr.Expr_ComprehensionExpr:
		comprehension := expression.GetComprehensionExpr()
		arr, err := parse(comprehension.IterRange)
		if err != nil {
			return nil, err
		}

		loopStep := comprehension.GetLoopStep()
		filter := loopStep.GetCallExpr().Args[0]
		fmt.Println("filter", filter)
		parsedFilter, err := parse(filter)
		fmt.Println("parsed", parsedFilter)
		if err != nil {
			return nil, err
		}

		q, ok := parsedFilter.(*Query)
		if !ok {
			return nil, fmt.Errorf("unable to cast %v to query type", parsedFilter)
		}
		path := arr.(string)
		qq := rewriteNestedQueryTerms(path, comprehension.IterVar,  q)

		return &Query{
			Nested: &Nested{
				Path: path,
				Query: qq,
				//Query: q,
			},
		}, nil

	case *expr.Expr_CallExpr:
		functionExpr := expression.GetCallExpr()

		switch functionExpr.Function {
		case operators.Equals:
			left, right, err := twoArityArguments(functionExpr)
			if err != nil {
				return nil, err
			}
			leftTerm := left.(string)
			rightTerm := right.(string)

			return &Query{
				Term: &Term{
					leftTerm: rightTerm,
				},
			}, nil
		case operators.NotEquals:
			left, right, err := twoArityArguments(functionExpr)
			if err != nil {
				return nil, err
			}
			leftTerm := left.(string)
			rightTerm := right.(string)

			return &Query{
				Bool: &Bool{
					MustNot: &MustNot{
						&Bool{
							Term: &Term{
								leftTerm: rightTerm,
							},
						},
					},
				},
			}, nil
		case operators.LogicalAnd:
			left, right, err := twoArityArguments(functionExpr)
			if err != nil {
				return nil, err
			}

			return &Query{
				Bool: &Bool{
					Must: &Must{
						left,
						right,
					},
				},
			}, nil
		case operators.LogicalOr:
			left, right, err := twoArityArguments(functionExpr)
			if err != nil {
				return nil, err
			}

			return &Query{
				Bool: &Bool{
					Should: &Should{
						left, //append left recursively and add to the must slice
						right, //append right recursively and add to the must slice
					},
				},
			}, nil

		case overloads.Contains:
			target, arg, err := targetAndArgument(functionExpr)
			if err != nil {
				return nil, err
			}
			targetStr := target.(string) // TODO: better type check
			argStr := arg.(string)

			query := fmt.Sprintf("*%s*", elasticsearchSpecialCharacterRegex.ReplaceAllString(argStr, `\$1`))

			return &Query{
				QueryString: &QueryString{
					DefaultField: targetStr,
					Query:        query,
				},
			}, nil

		case overloads.StartsWith:
			target, arg, err := targetAndArgument(functionExpr)
			if err != nil {
				return nil, err
			}
			targetStr := target.(string) // TODO: better type check
			argStr := arg.(string)

			q := &Query{
				Prefix: &Term{
					targetStr: argStr,
				},
			}

			return q, nil

		default:
			return nil, fmt.Errorf("unknown function: %s", functionExpr.Function)
		}

	default:
		return nil, fmt.Errorf("unknown expression: %v", expression)
	}

	//return nil, nil
}

// ParseExpression to parse and create a query
func parseExpression(expression *expr.Expr) (*Query, error) {
	function := expression.GetCallExpr().GetFunction()

	// Determine if left and right side are final and if so formulate query
	var leftArg, rightArg *expr.Expr

	if len(expression.GetCallExpr().Args) == 2 {
		// For the expression a == b, a and b are treated as arguments to the _==_ operator
		leftArg = expression.GetCallExpr().Args[0]
		rightArg = expression.GetCallExpr().Args[1]
	} else {
		// In the expression a.startsWith(b), a is the target/receiver and b is the argument.
		leftArg = expression.GetCallExpr().Target
		rightArg = expression.GetCallExpr().Args[0]
	}

	switch function {
	case operators.LogicalAnd:
		l, err := parseExpression(leftArg)
		if err != nil {
			return nil, err
		}
		r, err := parseExpression(rightArg)
		if err != nil {
			return nil, err
		}

		return &Query{
			Bool: &Bool{
				Must: &Must{
					l, //append left recursively and add to the must slice
					r, //append right recursively and add to the must slice
				},
			},
		}, nil
	case operators.LogicalOr:
		l, err := parseExpression(leftArg)
		if err != nil {
			return nil, err
		}
		r, err := parseExpression(rightArg)
		if err != nil {
			return nil, err
		}

		return &Query{
			Bool: &Bool{
				Should: &Should{
					l, //append left recursively and add to the must slice
					r, //append right recursively and add to the must slice
				},
			},
		}, nil
	case operators.Equals:
		leftTerm, rightTerm, err := getSimpleExpressionTerms(leftArg, rightArg)
		if err != nil {
			return nil, err
		}

		return &Query{
			Term: &Term{
				leftTerm: rightTerm,
			},
		}, nil
	case operators.NotEquals:
		leftTerm, rightTerm, err := getSimpleExpressionTerms(leftArg, rightArg)
		if err != nil {
			return nil, err
		}

		return &Query{
			Bool: &Bool{
				MustNot: &MustNot{
					&Bool{
						Term: &Term{
							leftTerm: rightTerm,
						},
					},
				},
			},
		}, nil
	case operators.Index:
		path := ""
		switch leftArg.ExprKind.(type) {
		case *expr.Expr_IdentExpr:
			path = leftArg.GetIdentExpr().Name
		case *expr.Expr_ConstExpr:
			path = leftArg.GetConstExpr().GetStringValue()
		}

		if path == "" {
			return nil, fmt.Errorf("unexpected path: %v", leftArg)
		}

		q, e := parseExpression(rightArg)
		if e != nil {
			return nil, e
		}

		rewrittenQuery := rewriteNestedQueryTerms(path,"", q)

		return &Query{
			Nested: &Nested{
				Path:  path,
				Query: rewrittenQuery,
			},
		}, nil
	case overloads.StartsWith:
		leftTerm, rightTerm, err := getSimpleExpressionTerms(leftArg, rightArg)
		if err != nil {
			return nil, err
		}

		return &Query{
			Prefix: &Term{
				leftTerm: rightTerm,
			},
		}, nil
	case overloads.Contains:
		leftTerm, rightTerm, err := getSimpleExpressionTerms(leftArg, rightArg)
		if err != nil {
			return nil, err
		}

		// special characters need to be escaped via "\"
		query := fmt.Sprintf("*%s*", elasticsearchSpecialCharacterRegex.ReplaceAllString(rightTerm, `\$1`))

		return &Query{
			QueryString: &QueryString{
				DefaultField: leftTerm,
				Query:        query,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown parse expression function: %s", function)
	}
}

func rewriteTerm(path, iter string, term *Term) *Term {
	for k, v := range *term {
		delete(*term, k)
		parts := strings.Split(k, ".")
		subPath := parts
		if parts[0] == iter {
			subPath = parts[1:]
		}
		subPathStr := strings.Join(subPath, ".")

		newKey := fmt.Sprintf("%s.%s", path, subPathStr)

		(*term)[newKey] = v
	}

	return term
}

func rewriteNestedQueryTerms(path, iter string, query *Query) *Query {
	if query.Term != nil {
		query.Term = rewriteTerm(path, iter, query.Term)
	}

	if query.Prefix != nil {
		query.Prefix = rewriteTerm(path, iter, query.Prefix)
	}

	return query
}

// converts left and right call expressions into simple term strings.
// this function should be used at the top of the `parseExpression` call stack.
func getSimpleExpressionTerms(leftArg, rightArg *expr.Expr) (leftTerm, rightTerm string, err error) {
	if _, ok := leftArg.GetExprKind().(*expr.Expr_IdentExpr); ok {
		leftTerm = leftArg.GetIdentExpr().Name
	}

	if _, ok := leftArg.GetExprKind().(*expr.Expr_ConstExpr); ok {
		leftTerm = leftArg.GetConstExpr().GetStringValue()
	}

	if _, ok := rightArg.GetExprKind().(*expr.Expr_IdentExpr); ok {
		rightTerm = rightArg.GetIdentExpr().Name
	}

	if _, ok := rightArg.GetExprKind().(*expr.Expr_ConstExpr); ok {
		rightTerm = rightArg.GetConstExpr().GetStringValue()
	}

	if leftTerm == "" || rightTerm == "" {
		err = fmt.Errorf("encountered unexpected expression kinds when evaluating filter: %T, %T", leftArg.GetExprKind(), rightArg.GetExprKind())
	}

	return
}
