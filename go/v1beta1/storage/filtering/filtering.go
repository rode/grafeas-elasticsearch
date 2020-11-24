package filtering

import (
	"encoding/json"

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/parser"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Query holds a parent query that carries the entire search query
type Query struct {
	Query interface{} `json:"query"`
}

// Bool holds a general query that carries any number of
// Must and Should operations
type Bool struct {
	Bool interface{} `json:"bool"`
}

// Must holds a must operator which each equates to an AND operation
type Must struct {
	Must []interface{} `json:"must"`
}

// Should holds a should operator which equates to an OR operation
type Should struct {
	Should []interface{} `json:"should"`
}

// Term holds a comparison for equating two strings
type Term struct {
	Term map[string]string `json:"term"`
}

//checkIfMustOrShould will return whether the operation is an AND (must) or OR (should)
func checkIfMustOrShould(function string) string {
	if function == "_&&_" {
		return "must"
	}
	if function == "_||_" {
		return "should"
	}
	return "equal"
}

// ParseExpressionEntrypoint will serve as the entrypoint to the filter
// that is eventually passed to parseExpression which will handle the recursive logic
func ParseExpressionEntrypoint(filter string) string {
	parsedExpr, _ := parser.Parse(common.NewStringSource(filter, ""))
	// function := parsedExpr.GetExpr().GetCallExpr().GetFunction()
	expression := parsedExpr.GetExpr()
	query := &Query{
		Query: map[string]interface{}{},
	}

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
			query.Query = &Bool{Bool: &Must{
				Must: []interface{}{parseExpression(expression)},
			},
			}
			queryBytes, _ := json.Marshal(query)
			return string(queryBytes)
		}
	}
	query.Query = parseExpression(expression)

	queryBytes, _ := json.Marshal(query)
	return string(queryBytes)
}

// ParseExpression to parse and create a query
func parseExpression(expression *expr.Expr) interface{} {
	_, isCallExpr := expression.ExprKind.(*expr.Expr_CallExpr)
	if isCallExpr {
		function := expression.GetCallExpr().GetFunction() // =

		// Determine if left and right side are final and if so formulate query
		leftarg := expression.GetCallExpr().Args[0]
		rightarg := expression.GetCallExpr().Args[1]

		// Check to see if each side is an identity or const expression
		_, leftIsIdent := leftarg.ExprKind.(*expr.Expr_IdentExpr)
		_, leftIsConst := leftarg.ExprKind.(*expr.Expr_ConstExpr)
		_, rightIsIdent := rightarg.ExprKind.(*expr.Expr_IdentExpr)
		_, rightIsConst := rightarg.ExprKind.(*expr.Expr_ConstExpr)

		// operationalTerm may hold a Must or Should
		var operationalTerm interface{}
		if checkIfMustOrShould(function) == "must" {
			var mustTerm []interface{}
			//append left
			mustTerm = append(mustTerm, parseExpression(leftarg))
			//append right
			mustTerm = append(mustTerm, parseExpression(rightarg))

			operationalTerm = &Must{
				Must: mustTerm,
			}
			return &Bool{Bool: operationalTerm}

		} else if checkIfMustOrShould(function) == "should" {
			var shouldTerm []interface{}
			shouldTerm = append(shouldTerm, parseExpression(leftarg))
			shouldTerm = append(shouldTerm, parseExpression(rightarg))
			operationalTerm = &Should{
				Should: shouldTerm,
			}
			return &Bool{Bool: operationalTerm}

		} else if checkIfMustOrShould(function) == "equal" {
			leftString := ""
			rightString := ""
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

			term := &Term{
				Term: map[string]string{
					leftString: rightString,
				},
			}
			return term

		}
	}
	return nil
}
