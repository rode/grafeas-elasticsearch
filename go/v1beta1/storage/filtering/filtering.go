package filtering

import (
	"github.com/google/cel-go/common"
	"github.com/google/cel-go/parser"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// ParseExpressionEntrypoint will serve as the entrypoint to the filter
// that is eventually passed to parseExpression which will handle the recursive logic
func ParseExpressionEntrypoint(filter string) (*Query, []common.Error) {
	parsedExpr, err := parser.Parse(common.NewStringSource(filter, ""))
	if len(err.GetErrors()) > 0 {
		return nil, err.GetErrors()
	}

	expression := parsedExpr.GetExpr()
	query := &Query{}

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
			query.Bool = &Bool{
				Must: &Must{parseExpression(expression)},
			}

			return query, nil
		}
	}

	query = parseExpression(expression).(*Query)

	return query, nil
}

// ParseExpression to parse and create a query
func parseExpression(expression *expr.Expr) interface{} {
	function := Operation(expression.GetCallExpr().GetFunction()) // =

	// Determine if left and right side are final and if so formulate query
	leftArg := expression.GetCallExpr().Args[0]
	rightArg := expression.GetCallExpr().Args[1]

	// Check to see if each side is an identity or const expression
	_, leftIsIdent := leftArg.ExprKind.(*expr.Expr_IdentExpr)
	_, leftIsConst := leftArg.ExprKind.(*expr.Expr_ConstExpr)
	_, rightIsIdent := rightArg.ExprKind.(*expr.Expr_IdentExpr)
	_, rightIsConst := rightArg.ExprKind.(*expr.Expr_ConstExpr)

	if function == AndOperation {
		return &Query{
			Bool: &Bool{
				Must: &Must{
					parseExpression(leftArg),  //append left recursively and add to the must slice
					parseExpression(rightArg), //append right recursively and add to the must slice
				},
			},
		}

	} else if function == OrOperation {
		return &Query{
			Bool: &Bool{
				Should: &Should{
					parseExpression(leftArg),  //append left recursively and add to the must slice
					parseExpression(rightArg), //append right recursively and add to the must slice
				},
			},
		}
	} else { // currently the else block is used for _==_ operations
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
		}
	}
}
