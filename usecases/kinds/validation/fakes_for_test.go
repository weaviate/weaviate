package validation

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
)

func testSchema() schema.Schema {
	return schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "Person",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "phone",
							DataType: []string{"phoneNumber"},
						},
					},
				},
			},
		},
	}
}

func fakeExists(context.Context, kind.Kind, strfmt.UUID) (bool, error) {
	return true, nil
}

type fakePeerLister struct{}

func (f *fakePeerLister) ListPeers() (peers.Peers, error) {
	return nil, nil
}
