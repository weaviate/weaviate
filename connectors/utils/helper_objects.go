/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package connutils

// RefTypeAction used for actions in DB and requests
const RefTypeAction string = "Action"

// RefTypeKey used for keys in DB and requests
const RefTypeKey string = "Key"

// RefTypeThing used for things in DB and requests
const RefTypeThing string = "Thing"

// Operator is a representation of the operator for queries
type Operator uint16

const (
	// Equal represents an operator for an operation to be equal
	Equal Operator = 1 << iota
	// NotEqual represents an operator for an operation to be unequal
	NotEqual
	// GreaterThan represents an operator for an operation to be greather than the value
	GreaterThan
	// GreaterThanEqual represents an operator for an operation to be greather or equal than the value
	GreaterThanEqual
	// LessThan represents an operator for an operation to be less than the value
	LessThan
	// LessThanEqual represents an operator for an operation to be less or equal than the value
	LessThanEqual
)

// ValueType is the type representing the value in the query
type ValueType struct {
	Value          interface{} // String-value / int-value / etc.
	Operator       Operator    // See Operator constants
	WildCardBefore bool        // Has wildcard before value
	WildCardAfter  bool        // Has wildcard after value
}

// WhereQuery represents the query itself
type WhereQuery struct {
	Property string
	Value    ValueType
}
