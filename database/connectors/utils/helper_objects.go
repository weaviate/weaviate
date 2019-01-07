/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */

package connutils

type (
	// Operator is a representation of the operator for queries
	Operator uint16
	// RefType is used to have a common name for the kind of items in Weaviate
	RefType string
)

const (
	// RefTypeAction used for actions in DB and requests
	RefTypeAction RefType = "Action"
	// RefTypeKey used for keys in DB and requests
	RefTypeKey RefType = "Key"
	// RefTypeThing used for things in DB and requests
	RefTypeThing RefType = "Thing"
	// RefTypeNetworkAction used for actions in DB and requests
	RefTypeNetworkAction RefType = "NetworkAction"
	// RefTypeNetworkThing used for things in DB and requests
	RefTypeNetworkThing RefType = "NetworkThing"

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

	// StaticNoRootKey message when no root key is found
	StaticNoRootKey string = "No root-key found."
	// StaticThingNotFound message when thing is not found
	StaticThingNotFound string = "Thing is not found in database"
	// StaticNoHistoryFound message for when no history is found
	StaticNoHistoryFound string = "No history is not found in database"
	// StaticActionNotFound message when action is not found
	StaticActionNotFound string = "Action is not found in database"
	// StaticKeyNotFound message when key is not found
	StaticKeyNotFound string = "Key is not found in database"

	// StaticMissingHeader message
	StaticMissingHeader string = "Please provide both X-API-KEY and X-API-TOKEN headers."
	// StaticInvalidToken message
	StaticInvalidToken string = "Provided token is invalid."
	// StaticKeyExpired message
	StaticKeyExpired string = "Provided key has expired."
)

// ValueType is the type representing the value in the query
type ValueType struct {
	Value    interface{} // String-value / int-value / etc.
	Operator Operator    // See Operator constants
	Contains bool        // Has 'contains' mark
}

// WhereQuery represents the query itself
type WhereQuery struct {
	Property string
	Value    ValueType
}
