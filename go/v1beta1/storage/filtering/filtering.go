package filtering

import (
	"fmt"
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
	default:
		return nil, fmt.Errorf("unknown parse expression function: %s", function)
	}
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
