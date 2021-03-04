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
	"fmt"
	"regexp"

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/parser"
	"github.com/hashicorp/go-multierror"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type Filterer interface {
	ParseExpression(filter string) (*Query, error)
}

type filterer struct{}

func NewFilterer() Filterer {
	return &filterer{}
}

var elasticsearchSpecialCharacterRegex = regexp.MustCompile(`([\-=&|!(){}\[\]^"~*?:\\/])`)

// ParseExpression will serve as the entrypoint to the filter
// that is eventually passed to visit which will handle the recursive logic
func (f *filterer) ParseExpression(filter string) (*Query, error) {
	parsedExpr, commonErr := parser.Parse(common.NewStringSource(filter, ""))
	if len(commonErr.GetErrors()) > 0 {
		resultErr := fmt.Errorf("error parsing filter")
		for _, e := range commonErr.GetErrors() {
			resultErr = multierror.Append(resultErr, fmt.Errorf("%s (%d:%d)", e.Message, e.Location.Line(), e.Location.Column()))
		}

		return nil, resultErr
	}

	maybeQuery, err := f.visit(parsedExpr.GetExpr(), "")
	if err != nil {
		return nil, err
	}

	query, ok := maybeQuery.(*Query)
	if !ok {
		return nil, fmt.Errorf("source did not result in a valid Elasticsearch query")
	}

	return query, nil
}

func (f *filterer) visit(expression *expr.Expr, depth string) (interface{}, error) {
	switch expression.ExprKind.(type) {
	case *expr.Expr_IdentExpr:
		return f.visitIdent(expression, depth)
	case *expr.Expr_ConstExpr:
		return f.visitConst(expression, depth)
	case *expr.Expr_SelectExpr:
		return f.visitSelect(expression, depth)
	case *expr.Expr_CallExpr:
		return f.visitCall(expression, depth)
	default:
		return nil, fmt.Errorf("unrecognized expression: %v", expression)
	}
}

func (f *filterer) visitIdent(expression *expr.Expr, _ string) (interface{}, error) {
	return expression.GetIdentExpr().Name, nil
}

func (f *filterer) visitConst(expression *expr.Expr, _ string) (interface{}, error) {
	constantExpr := expression.GetConstExpr()

	var value interface{}

	switch constantExpr.ConstantKind.(type) {
	case *expr.Constant_BoolValue:
		value = constantExpr.GetBoolValue()
	case *expr.Constant_StringValue:
		value = constantExpr.GetStringValue()
	case *expr.Constant_Int64Value:
		value = constantExpr.GetInt64Value()
	case *expr.Constant_Uint64Value:
		value = constantExpr.GetUint64Value()
	default:
		return nil, fmt.Errorf("unrecognized constant kind %T", constantExpr.ConstantKind)
	}

	return value, nil
}

func (f *filterer) visitSelect(expression *expr.Expr, depth string) (interface{}, error) {
	selectExp := expression.GetSelectExpr()

	value, err := f.visit(selectExp.Operand, depth)
	if err != nil {
		return nil, err
	}
	if depth != "" {
		return fmt.Sprintf("%s.%s.%s", depth, value, selectExp.Field), nil
	}

	return fmt.Sprintf("%s.%s", value, selectExp.Field), nil
}

func (f *filterer) visitCall(expression *expr.Expr, depth string) (interface{}, error) {
	function := expression.GetCallExpr().Function
	switch function {
	case operators.LogicalAnd,
		operators.LogicalOr,
		operators.Equals,
		operators.NotEquals:
		return f.visitBinaryOperator(expression, depth)
	case overloads.Contains,
		overloads.StartsWith:
		return f.visitCallFunction(expression, depth)
	default:
		return nil, fmt.Errorf("unrecognized function: %s", function)
	}
}

func (f *filterer) visitBinaryOperator(expression *expr.Expr, depth string) (interface{}, error) {
	args := expression.GetCallExpr().Args

	if len(args) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to binary operator")
	}

	left := args[0]
	right := args[1]

	lhs, err := f.visit(left, depth)
	if err != nil {
		return nil, err
	}

	rhs, err := f.visit(right, depth)
	if err != nil {
		return nil, err
	}

	switch expression.GetCallExpr().Function {
	case operators.LogicalAnd:
		return &Query{
			Bool: &Bool{
				Must: &Must{
					lhs,
					rhs,
				},
			},
		}, nil
	case operators.LogicalOr:
		return &Query{
			Bool: &Bool{
				Should: &Should{
					lhs,
					rhs,
				},
			},
		}, nil
	case operators.Equals:
		leftTerm, err := assertString(lhs)
		if err != nil {
			return nil, err
		}

		rightTerm, err := assertString(rhs)
		if err != nil {
			return nil, err
		}

		return &Query{
			Term: &Term{
				leftTerm: rightTerm,
			},
		}, nil
	case operators.NotEquals:
		leftTerm, err := assertString(lhs)
		if err != nil {
			return nil, err
		}

		rightTerm, err := assertString(rhs)
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
	}

	return nil, fmt.Errorf("unrecognized function %s", expression.GetCallExpr().Function)
}

func (f *filterer) visitCallFunction(expression *expr.Expr, depth string) (interface{}, error) {
	callExpr := expression.GetCallExpr()
	targetExpr := callExpr.Target

	target, err := f.visit(targetExpr, depth)
	if err != nil {
		return nil, err
	}

	if len(callExpr.Args) != 1 {
		return nil, fmt.Errorf("invalid number of arguments")
	}

	argExpr := callExpr.Args[0]
	arg, err := f.visit(argExpr, depth)
	if err != nil {
		return nil, err
	}

	t, err := assertString(target)
	if err != nil {
		return nil, err
	}

	a, err := assertString(arg)
	if err != nil {
		return nil, err
	}

	switch callExpr.Function {
	case overloads.StartsWith:
		return &Query{
			Prefix: &Term{
				t: a,
			},
		}, nil
	case overloads.Contains:
		return &Query{
			QueryString: &QueryString{
				DefaultField: t,
				Query:        fmt.Sprintf("*%s*", elasticsearchSpecialCharacterRegex.ReplaceAllString(a, `\$1`)),
			},
		}, nil
	}

	return nil, fmt.Errorf("unrecognized function: %s", callExpr.Function)
}

func assertString(value interface{}) (string, error) {
	stringValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("not a string")
	}

	return stringValue, nil
}
