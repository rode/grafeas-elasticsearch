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

	query.Bool = parseExpression(expression).(*Bool)

	return query, nil
}

// ParseExpression to parse and create a query
func parseExpression(expression *expr.Expr) interface{} {
	function := Operation(expression.GetCallExpr().GetFunction()) // =

	// Determine if left and right side are final and if so formulate query
	leftarg := expression.GetCallExpr().Args[0]
	rightarg := expression.GetCallExpr().Args[1]

	// Check to see if each side is an identity or const expression
	_, leftIsIdent := leftarg.ExprKind.(*expr.Expr_IdentExpr)
	_, leftIsConst := leftarg.ExprKind.(*expr.Expr_ConstExpr)
	_, rightIsIdent := rightarg.ExprKind.(*expr.Expr_IdentExpr)
	_, rightIsConst := rightarg.ExprKind.(*expr.Expr_ConstExpr)

	if function == AndOperation {
		return &Bool{
			Must: &Must{
				parseExpression(leftarg),  //append left recursively and add to the must slice
				parseExpression(rightarg), //append right recursively and add to the must slice
			},
		}

	} else if function == OrOperation {
		return &Bool{
			Should: &Should{
				parseExpression(leftarg),  //append left recursively and add to the must slice
				parseExpression(rightarg), //append right recursively and add to the must slice
			},
		}
	} else { // currently the else block is used for _==_ operations
		var leftString string
		var rightString string
		if leftIsIdent && rightIsConst {
			leftString = leftarg.GetIdentExpr().Name
			rightString = rightarg.GetConstExpr().GetStringValue()
		} else if leftIsConst && rightIsConst {
			leftString = leftarg.GetConstExpr().GetStringValue()
			rightString = rightarg.GetConstExpr().GetStringValue()
		} else if leftIsIdent && rightIsIdent {
			leftString = leftarg.GetIdentExpr().Name
			rightString = rightarg.GetIdentExpr().Name
		} else if leftIsConst && rightIsIdent {
			leftString = leftarg.GetConstExpr().GetStringValue()
			rightString = rightarg.GetIdentExpr().Name
		}
		return &Term{
			leftString: rightString,
		}
	}
}
