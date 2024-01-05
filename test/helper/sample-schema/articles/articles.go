//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package articles

import (
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func ArticlesClass() *models.Class {
	return &models.Class{
		Class: "Article",
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "hasParagraphs",
				DataType: []string{"Paragraph"},
			},
		},
	}
}

func ParagraphsClass() *models.Class {
	return &models.Class{
		Class: "Paragraph",
		Properties: []*models.Property{
			{
				Name:     "contents",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		Vectorizer: "none",
	}
}

type Article models.Object

func (a *Article) WithID(id strfmt.UUID) *Article {
	a.ID = id
	return a
}

func (a *Article) WithReferences(refs ...*models.SingleRef) *Article {
	props := a.Properties.(map[string]interface{})
	props["hasParagraphs"] = models.MultipleRef(refs)
	return a
}

func (a *Article) WithTitle(title string) *Article {
	props := a.Properties.(map[string]interface{})
	props["title"] = title
	return a
}

func (a *Article) WithTenant(tk string) *Article {
	a.Tenant = tk
	return a
}

func (a *Article) Object() *models.Object {
	obj := models.Object(*a)
	return &obj
}

func NewArticle() *Article {
	return &Article{
		Class:      "Article",
		ID:         strfmt.UUID(uuid.NewString()),
		Properties: make(map[string]interface{}),
	}
}

type Paragraph models.Object

func (p *Paragraph) WithID(id strfmt.UUID) *Paragraph {
	p.ID = id
	return p
}

func (p *Paragraph) WithContents(contents string) *Paragraph {
	props := p.Properties.(map[string]interface{})
	props["contents"] = contents
	return p
}

func (p *Paragraph) WithVector(vec []float32) *Paragraph {
	p.Vector = vec
	return p
}

func (p *Paragraph) WithTenant(tk string) *Paragraph {
	p.Tenant = tk
	return p
}

func (p *Paragraph) Object() *models.Object {
	obj := models.Object(*p)
	return &obj
}

func NewParagraph() *Paragraph {
	return &Paragraph{
		Class:      "Paragraph",
		ID:         strfmt.UUID(uuid.NewString()),
		Properties: make(map[string]interface{}),
	}
}
