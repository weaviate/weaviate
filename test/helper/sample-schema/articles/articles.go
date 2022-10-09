//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package articles

import (
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/models"
)

func ArticlesClass() *models.Class {
	return &models.Class{
		Class: "Article",
		ModuleConfig: map[string]interface{}{
			"ref2vec-centroid": map[string]interface{}{
				"referenceProperties": []string{"hasParagraphs"},
			},
		},
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"string"},
			},
			{
				Name:     "hasParagraphs",
				DataType: []string{"Paragraph"},
			},
		},
		Vectorizer: "ref2vec-centroid",
	}
}

func ParagraphsClass() *models.Class {
	return &models.Class{
		Class: "Paragraph",
		Properties: []*models.Property{
			{
				Name:     "contents",
				DataType: []string{"text"},
			},
		},
		Vectorizer: "none",
	}
}

type Article models.Object

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

func (p *Paragraph) WithContents(contents string) *Paragraph {
	props := p.Properties.(map[string]interface{})
	props["contents"] = contents
	return p
}

func (p *Paragraph) WithVector(vec []float32) *Paragraph {
	p.Vector = vec
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
