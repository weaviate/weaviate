package janusgraph

import (
	"encoding/json"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"strconv"
)

// Stores the internal JanusGraph connector state.
// - the version of the connector state
// - the mappings of class names and property names
type janusGraphConnectorState struct {
	Version int64 `json:"version"`
	lastId  int64 `json:"next_id"`

	ClassMap    map[schema.ClassName]MappedClassName                            `json:"classMap"`
	PropertyMap map[schema.ClassName]map[schema.PropertyName]MappedPropertyName `json:"propertyMap"`
}

type MappedClassName string
type MappedPropertyName string

// Generates a new hex number based on the lastId.
// Increments lastId, does _not_ commit the internal state;
// you probably want to use this function whilst modifying more,
// so that would trigger an unnecessary sync across all instances.
func (s *janusGraphConnectorState) getNextId() string {
	s.lastId += 1
	return strconv.FormatInt(s.lastId, 16)
}

// Add a mapping of a classname to mapped name.
func (s *janusGraphConnectorState) addMappedClassName(className schema.ClassName) MappedClassName {
	_, exists := s.ClassMap[className]

	if exists {
		panic(fmt.Sprintf("Fatal error; class name %v is already mapped to a janus name", className))
	}

	mappedName := MappedClassName(fmt.Sprintf("class_%s", s.getNextId()))
	s.ClassMap[className] = mappedName
	return mappedName
}

// Map a schema name to the internal janusgraph name
func (s *janusGraphConnectorState) getMappedClassName(className schema.ClassName) MappedClassName {
	mappedName, exists := s.ClassMap[className]

	if !exists {
		panic(fmt.Sprintf("Fatal error; class name %v is not mapped to a janus name", className))
	}

	return mappedName
}

// Remove mapped class name, and all properties.
func (s *janusGraphConnectorState) removeMappedClassName(className schema.ClassName) {
	delete(s.ClassMap, className)
	delete(s.PropertyMap, className)
}

// Add mapping from class/property name to mapped property namej
func (s *janusGraphConnectorState) addMappedPropertyName(className schema.ClassName, propName schema.PropertyName) MappedPropertyName {
	propsOfClass, exists := s.PropertyMap[className]

	if !exists {
		propsOfClass := make(map[schema.PropertyName]MappedPropertyName)
		s.PropertyMap[className] = propsOfClass
	}

	_, exists = propsOfClass[propName]
	if exists {
		panic(fmt.Sprintf("Fatal error; property %v for class name %v is already mapped to a janus name", propName, className))
	}

	mappedName := MappedPropertyName(fmt.Sprintf("prop_%s", s.getNextId()))
	propsOfClass[propName] = mappedName
	return mappedName
}

// Map a schema name to the internal janusgraph name
func (s *janusGraphConnectorState) getMappedPropertyName(className schema.ClassName, propName schema.PropertyName) MappedPropertyName {
	propsOfClass, exists := s.PropertyMap[className]

	if !exists {
		panic(fmt.Sprintf("Fatal error; class name %v does not have mapped properties", className))
	}

	mappedName, exists := propsOfClass[propName]
	if !exists {
		panic(fmt.Sprintf("Fatal error; property %v for class name %v is not mapped to a janus name", propName, className))
	}

	return mappedName
}

// Called by a connector when it has updated it's internal state that needs to be shared across all connectors in other Weaviate instances.
func (j *Janusgraph) SetState(state json.RawMessage) {
	err := json.Unmarshal(state, &j.state)

	// Extra assertions if the connector has been initialized already.
	if j.initialized {
		if err != nil {
			panic(fmt.Sprintf("Could not deserialize a schema update, after Weaviate was initialized. Are you running multiple versions of Weaviate in the same cluster? Reason: %s", err.Error()))
		}
		if j.state.Version != SCHEMA_VERSION {
			panic(fmt.Sprintf("Received a schema update of version %v. We can only handle schema version %v. Are you running multiple versions of Weaviate in the same cluster?", j.state.Version, SCHEMA_VERSION))
		}
	} else {
		if err != nil {
			panic(fmt.Sprintf("Received an illegal JSON document as the connector state during initialization. Cannot recover from this. Error: %v", err))
		}
	}
}

func (j *Janusgraph) UpdateStateInStateManager() {
	rawState, err := json.Marshal(j.state)

	if err != nil {
		panic("Could not serialize internal state to json")
	}

	j.stateManager.SetState(rawState)
}
