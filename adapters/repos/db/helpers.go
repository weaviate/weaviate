package db

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type KindObject struct {
	kind   kind.Kind      `json:"kind"`
	thing  *models.Thing  `json:"thing"`
	action *models.Action `json:"action"`
	vector []float32      `json:"vector"`
}

func NewKindObjectFromThing(thing *models.Thing, vector []float32) *KindObject {
	return &KindObject{
		kind:   kind.Thing,
		thing:  thing,
		vector: vector,
	}
}

func NewKindObjectFromAction(action *models.Action, vector []float32) *KindObject {
	return &KindObject{
		kind:   kind.Action,
		action: action,
		vector: vector,
	}
}

func (ko *KindObject) Class() schema.ClassName {
	switch ko.kind {
	case kind.Thing:
		return schema.ClassName(ko.thing.Class)
	case kind.Action:
		return schema.ClassName(ko.action.Class)
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) CreationTimeUnix() int64 {
	switch ko.kind {
	case kind.Thing:
		return ko.thing.CreationTimeUnix
	case kind.Action:
		return ko.action.CreationTimeUnix
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) ID() strfmt.UUID {
	switch ko.kind {
	case kind.Thing:
		return ko.thing.ID
	case kind.Action:
		return ko.action.ID
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) LastUpdateTimeUnix() int64 {
	switch ko.kind {
	case kind.Thing:
		return ko.thing.LastUpdateTimeUnix
	case kind.Action:
		return ko.action.LastUpdateTimeUnix
	default:
		panic("impossible kind")
	}
}
func (ko *KindObject) Meta() *models.ObjectMeta {
	switch ko.kind {
	case kind.Thing:
		return ko.thing.Meta
	case kind.Action:
		return ko.action.Meta
	default:
		panic("impossible kind")
	}

}
func (ko *KindObject) Schema() models.PropertySchema {
	switch ko.kind {
	case kind.Thing:
		return ko.thing.Schema
	case kind.Action:
		return ko.action.Schema
	default:
		panic("impossible kind")
	}

}
func (ko *KindObject) VectorWeights() models.VectorWeights {
	switch ko.kind {
	case kind.Thing:
		return ko.thing.VectorWeights
	case kind.Action:
		return ko.action.VectorWeights
	default:
		panic("impossible kind")
	}
}

func (ko *KindObject) Kind() kind.Kind {
	return ko.kind
}
