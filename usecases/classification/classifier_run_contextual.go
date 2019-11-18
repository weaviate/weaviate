package classification

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (c *Classifier) classifyItemContextual(item search.Result, kind kind.Kind, params models.Classification) error {
	s := c.schemaGetter.GetSchemaSkipAuth()
	// safe to skip nil-check as we passed validation

	var classified []string
	for _, propName := range params.ClassifyProperties {
		prop, err := s.GetProperty(kind, schema.ClassName(params.Class), schema.PropertyName(propName))
		if err != nil {
			return fmt.Errorf("contextual: get target prop '%s': %v", propName, err)
		}

		dataType, err := s.FindPropertyDataType(prop.DataType)
		if err != nil {
			return fmt.Errorf("contextual: extract dataType of prop '%s': %v", propName, err)
		}

		// we have passed validation, so it is safe to assume that this is a ref prop
		targetClasses := dataType.Classes()

		// len=1 is guaranteed from validation
		targetClass := targetClasses[0]
		targetKind, _ := s.GetKindOfClass(targetClass)

		res, err := c.vectorRepo.VectorClassSearch(context.Background(), traverser.GetParams{
			SearchVector: item.Vector,
			ClassName:    targetClass.String(),
			Kind:         targetKind,
			Pagination: &filters.Pagination{
				Limit: 1,
			},
			Properties: traverser.SelectProperties{
				traverser.SelectProperty{
					Name: "uuid",
				},
			},
		})
		if err != nil {
			return fmt.Errorf("contextual: search closest target: %v", err)
		}

		if res == nil || len(res) == 0 {
			return fmt.Errorf("contexual: no potential targets found of class '%s' (%s)", targetClass, targetKind)
		}

		targetBeacon := crossref.New("localhost", res[0].ID, res[0].Kind).String()
		item.Schema.(map[string]interface{})[propName] = models.MultipleRef{
			&models.SingleRef{
				Beacon: strfmt.URI(targetBeacon),
				Meta: &models.ReferenceMeta{
					Classification: &models.ReferenceMetaClassification{
						WinningDistance: float64(9000), // TODO
					},
				},
			},
		}

		// append list of actually classified (can differ from scope!) properties,
		// so we can build the object meta information
		classified = append(classified, propName)
	}

	c.extendItemWithObjectMeta(&item, params, classified)
	err := c.store(item)
	if err != nil {
		return fmt.Errorf("store %s/%s: %v", item.ClassName, item.ID, err)
	}

	return nil
}
