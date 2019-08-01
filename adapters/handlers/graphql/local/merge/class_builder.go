package merge

import (
	"fmt"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

type classBuilder struct {
	schema          *schema.Schema
	peers           peers.Peers
	knownClasses    map[string]*graphql.Object
	knownRefClasses refclasses.ByNetworkClass
	beaconClass     *graphql.Object
	logger          logrus.FieldLogger
}

func newClassBuilder(schema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger,
	knownClasses map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass, beaconClass *graphql.Object,
) *classBuilder {
	b := &classBuilder{}

	b.logger = logger
	b.schema = schema
	b.peers = peers
	b.knownClasses = knownClasses
	b.knownRefClasses = knownRefClasses
	b.beaconClass = beaconClass

	return b
}

func (b *classBuilder) actions() (*graphql.Object, error) {
	return b.kinds(kind.Action, b.schema.Actions)
}

func (b *classBuilder) things() (*graphql.Object, error) {
	return b.kinds(kind.Thing, b.schema.Things)
}

func (b *classBuilder) kinds(k kind.Kind, kindSchema *models.Schema) (*graphql.Object, error) {
	classFields := graphql.Fields{}
	kindName := strings.Title(k.Name())

	for _, class := range kindSchema.Classes {
		classField, err := b.classField(k, class)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalMerge%ssObj", kindName),
		Fields:      classFields,
		Description: fmt.Sprintf(descriptions.LocalMergeThingsActionsObj, kindName),
	})

	return classes, nil
}

func (b *classBuilder) classField(k kind.Kind, class *models.Class) (*graphql.Field, error) {
	c, ok := b.knownClasses[class.Class]
	if !ok {
		return nil, fmt.Errorf("merge: class %s not found in known classes", class.Class)
	}
	classField := buildMergeClassField(c, k, class)
	return &classField, nil
}
