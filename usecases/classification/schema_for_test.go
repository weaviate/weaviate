package classification

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func testSchema() schema.Schema {
	return schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "ExactCategory",
				},
				&models.Class{
					Class: "MainCategory",
				},
				&models.Class{
					Class: "Article",
					Properties: []*models.Property{
						&models.Property{
							Name:     "description",
							DataType: []string{string(schema.DataTypeText)},
						},
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "exactCategory",
							DataType: []string{"ExactCategory"},
						},
						&models.Property{
							Name:     "mainCategory",
							DataType: []string{"MainCatagory"},
						},
					},
				},
			},
		},
	}
}
