package filtering

import (
	"fmt"
	"github.com/google/cel-go/common"
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

	_, isCallExpr := expression.ExprKind.(*expr.Expr_CallExpr)
	if isCallExpr {
		// Determine if left and right side are final and if so formulate query
		leftarg := expression.GetCallExpr().Args[0]
		rightarg := expression.GetCallExpr().Args[1]

		// Check to see if each side is an identity or const expression
		_, leftIsIdent := leftarg.ExprKind.(*expr.Expr_IdentExpr)
		_, leftIsConst := leftarg.ExprKind.(*expr.Expr_ConstExpr)
		_, rightIsIdent := rightarg.ExprKind.(*expr.Expr_IdentExpr)
		_, rightIsConst := rightarg.ExprKind.(*expr.Expr_ConstExpr)

		if (leftIsConst || leftIsIdent) && (rightIsConst || rightIsIdent) {
			pe, err := parseExpression(expression)
			if err != nil {
				return nil, err
			}

			return &Query{Bool: &Bool{
				Must: &Must{pe},
			}}, nil
		}
	}

	pe, err := parseExpression(expression)
	if err != nil {
		return nil, err
	}

	query, ok := pe.(*Query)
	if !ok {
		return nil, fmt.Errorf("cannot cast parse expression result")
	}

	return query, nil
}

// ParseExpression to parse and create a query
func parseExpression(expression *expr.Expr) (interface{}, error) {
	function := Operation(expression.GetCallExpr().GetFunction()) // =

	// Determine if left and right side are final and if so formulate query
	leftArg := expression.GetCallExpr().Args[0]
	rightArg := expression.GetCallExpr().Args[1]

	// Check to see if each side is an identity or const expression
	_, leftIsIdent := leftArg.ExprKind.(*expr.Expr_IdentExpr)
	_, leftIsConst := leftArg.ExprKind.(*expr.Expr_ConstExpr)
	_, rightIsIdent := rightArg.ExprKind.(*expr.Expr_IdentExpr)
	_, rightIsConst := rightArg.ExprKind.(*expr.Expr_ConstExpr)

	switch function {
	case AndOperation:
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
	case OrOperation:
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
	case EqualOperation:
		var leftString string
		var rightString string
		if leftIsIdent && rightIsConst {
			leftString = leftArg.GetIdentExpr().Name
			rightString = rightArg.GetConstExpr().GetStringValue()
		} else if leftIsConst && rightIsConst {
			leftString = leftArg.GetConstExpr().GetStringValue()
			rightString = rightArg.GetConstExpr().GetStringValue()
		} else if leftIsIdent && rightIsIdent {
			leftString = leftArg.GetIdentExpr().Name
			rightString = rightArg.GetIdentExpr().Name
		} else if leftIsConst && rightIsIdent {
			leftString = leftArg.GetConstExpr().GetStringValue()
			rightString = rightArg.GetIdentExpr().Name
		}
		return &Bool{
			Term: &Term{
				leftString: rightString,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown parse expression function")
	}
}
