package db

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

type KindObject struct {
	Kind   kind.Kind      `json:"kind"`
	Thing  *models.Thing  `json:"thing"`
	Action *models.Action `json:"action"`
	Vector []float32      `json:"vector"`
}

func NewKindObjectFromThing(thing *models.Thing, vector []float32) *KindObject {
	return &KindObject{
		Kind:   kind.Thing,
		Thing:  thing,
		Vector: vector,
	}
}

func NewKindObjectFromAction(action *models.Action, vector []float32) *KindObject {
	return &KindObject{
		Kind:   kind.Action,
		Action: action,
		Vector: vector,
	}
}

func (ko *KindObject) Class() schema.ClassName {
	switch ko.Kind {
	case kind.Thing:
		return schema.ClassName(ko.Thing.Class)
	case kind.Action:
		return schema.ClassName(ko.Action.Class)
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) CreationTimeUnix() int64 {
	switch ko.Kind {
	case kind.Thing:
		return ko.Thing.CreationTimeUnix
	case kind.Action:
		return ko.Action.CreationTimeUnix
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) ID() strfmt.UUID {
	switch ko.Kind {
	case kind.Thing:
		return ko.Thing.ID
	case kind.Action:
		return ko.Action.ID
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) LastUpdateTimeUnix() int64 {
	switch ko.Kind {
	case kind.Thing:
		return ko.Thing.LastUpdateTimeUnix
	case kind.Action:
		return ko.Action.LastUpdateTimeUnix
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) Meta() *models.ObjectMeta {
	switch ko.Kind {
	case kind.Thing:
		return ko.Thing.Meta
	case kind.Action:
		return ko.Action.Meta
	default:
		panic("impossible kind")
	}

}
func (ko *KindObject) Schema() models.PropertySchema {
	switch ko.Kind {
	case kind.Thing:
		return ko.Thing.Schema
	case kind.Action:
		return ko.Action.Schema
	default:
		panic("impossible kind")
	}

}
func (ko *KindObject) VectorWeights() models.VectorWeights {
	switch ko.Kind {
	case kind.Thing:
		return ko.Thing.VectorWeights
	case kind.Action:
		return ko.Action.VectorWeights
	default:
		panic("impossible kind")
	}
}

func (ko *KindObject) SearchResult() *search.Result {
	return &search.Result{
		Kind:      ko.Kind,
		ID:        ko.ID(),
		ClassName: ko.Class().String(),
		Schema:    ko.Schema(),
		Vector:    ko.Vector,
		// VectorWeights: ko.VectorWeights(), // TODO: add vector weights
		Created: ko.CreationTimeUnix(),
		Updated: ko.LastUpdateTimeUnix(),
		Meta:    ko.Meta(),
		Score:   1, // TODO: actuallly score
		// TODO: Beacon?
	}
}
