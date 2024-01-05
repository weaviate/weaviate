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

package books

import (
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const defaultClassName = "Books"

const (
	Dune                  strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e000"
	ProjectHailMary       strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e001"
	TheLordOfTheIceGarden strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e002"
)

func ClassContextionaryVectorizer() *models.Class {
	return class(defaultClassName, "text2vec-contextionary")
}

func ClassContextionaryVectorizerWithName(className string) *models.Class {
	return class(className, "text2vec-contextionary")
}

func ClassContextionaryVectorizerWithSumTransformers() *models.Class {
	return class(defaultClassName, "text2vec-contextionary", "sum-transformers")
}

func ClassContextionaryVectorizerWithQnATransformers() *models.Class {
	return class(defaultClassName, "text2vec-contextionary", "qna-transformers")
}

func ClassTransformersVectorizer() *models.Class {
	return class(defaultClassName, "text2vec-transformers")
}

func ClassTransformersVectorizerWithName(className string) *models.Class {
	return class(className, "text2vec-transformers")
}

func ClassTransformersVectorizerWithQnATransformersWithName(className string) *models.Class {
	return class(className, "text2vec-transformers", "qna-transformers")
}

func ClassCLIPVectorizer() *models.Class {
	c := class(defaultClassName, "multi2vec-clip")
	c.ModuleConfig.(map[string]interface{})["multi2vec-clip"] = map[string]interface{}{
		"textFields": []string{"title", "tags", "description"},
	}
	return c
}

func class(className, vectorizer string, additionalModules ...string) *models.Class {
	moduleConfig := map[string]interface{}{
		vectorizer: map[string]interface{}{
			"vectorizeClassName": true,
		},
	}
	if len(additionalModules) > 0 {
		for _, module := range additionalModules {
			moduleConfig[module] = map[string]interface{}{}
		}
	}
	return &models.Class{
		Class:        className,
		Vectorizer:   vectorizer,
		ModuleConfig: moduleConfig,
		InvertedIndexConfig: &models.InvertedIndexConfig{
			IndexNullState:      true,
			IndexTimestamps:     true,
			IndexPropertyLength: true,
		},
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{vectorizer: map[string]interface{}{"skip": false}},
			},
			{
				Name:         "tags",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{vectorizer: map[string]interface{}{"skip": false}},
			},
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{vectorizer: map[string]interface{}{"skip": false}},
			},
			{
				Name: "meta", DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "isbn", DataType: schema.DataTypeText.PropString()},
					{
						Name: "obj", DataType: schema.DataTypeObject.PropString(),
						NestedProperties: []*models.NestedProperty{{Name: "text", DataType: schema.DataTypeText.PropString()}},
					},
					{
						Name: "objs", DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{{Name: "text", DataType: schema.DataTypeText.PropString()}},
					},
				},
			},
			{
				Name: "reviews", DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{Name: "tags", DataType: schema.DataTypeTextArray.PropString()}},
			},
		},
	}
}

func Objects() []*models.Object {
	return objects(defaultClassName)
}

func BatchObjects() []*pb.BatchObject {
	return batchObjects(defaultClassName)
}

func ObjectsWithName(className string) []*models.Object {
	return objects(className)
}

func objects(className string) []*models.Object {
	return []*models.Object{
		{
			Class: className,
			ID:    Dune,
			Properties: map[string]interface{}{
				"title":       "Dune",
				"description": "Dune is a 1965 epic science fiction novel by American author Frank Herbert.",
			},
		},
		{
			Class: className,
			ID:    ProjectHailMary,
			Properties: map[string]interface{}{
				"title":       "Project Hail Mary",
				"description": "Project Hail Mary is a 2021 science fiction novel by American novelist Andy Weir.",
			},
		},
		{
			Class: className,
			ID:    TheLordOfTheIceGarden,
			Properties: map[string]interface{}{
				"title":       "The Lord of the Ice Garden",
				"tags":        []string{"three", "three", "three"},
				"description": "The Lord of the Ice Garden (Polish: Pan Lodowego Ogrodu) is a four-volume science fiction and fantasy novel by Polish writer Jaroslaw Grzedowicz.",
			},
		},
	}
}

func batchObjects(className string) []*pb.BatchObject {
	scifi := "sci-fi"
	return []*pb.BatchObject{
		{
			Collection: className,
			Uuid:       Dune.String(),
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"title":       structpb.NewStringValue("Dune"),
						"description": structpb.NewStringValue("Dune is a 1965 epic science fiction novel by American author Frank Herbert."),
					},
				},
				ObjectProperties: []*pb.ObjectProperties{{
					PropName: "meta",
					Value: &pb.ObjectPropertiesValue{
						NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"isbn": structpb.NewStringValue("978-0593099322")}},
						ObjectProperties: []*pb.ObjectProperties{{
							PropName: "obj",
							Value: &pb.ObjectPropertiesValue{
								NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue("some text")}},
							},
						}},
						ObjectArrayProperties: []*pb.ObjectArrayProperties{{
							PropName: "objs",
							Values: []*pb.ObjectPropertiesValue{{
								NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue("some text")}},
							}},
						}},
					},
				}},
				ObjectArrayProperties: []*pb.ObjectArrayProperties{{
					PropName: "reviews",
					Values:   []*pb.ObjectPropertiesValue{{TextArrayProperties: []*pb.TextArrayProperties{{PropName: "tags", Values: []string{scifi, "epic"}}}}},
				}},
			},
		},
		{
			Collection: className,
			Uuid:       ProjectHailMary.String(),
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"title":       structpb.NewStringValue("Project Hail Mary"),
						"description": structpb.NewStringValue("Project Hail Mary is a 2021 science fiction novel by American novelist Andy Weir."),
					},
				},
				ObjectProperties: []*pb.ObjectProperties{{
					PropName: "meta",
					Value: &pb.ObjectPropertiesValue{
						NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"isbn": structpb.NewStringValue("978-0593135204")}},
						ObjectProperties: []*pb.ObjectProperties{{
							PropName: "obj",
							Value:    &pb.ObjectPropertiesValue{NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue("some text")}}},
						}},
						ObjectArrayProperties: []*pb.ObjectArrayProperties{{
							PropName: "objs",
							Values: []*pb.ObjectPropertiesValue{{
								NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue("some text")}},
							}},
						}},
					},
				}},
				ObjectArrayProperties: []*pb.ObjectArrayProperties{{
					PropName: "reviews",
					Values:   []*pb.ObjectPropertiesValue{{TextArrayProperties: []*pb.TextArrayProperties{{PropName: "tags", Values: []string{scifi}}}}},
				}},
			},
		},
		{
			Collection: className,
			Uuid:       TheLordOfTheIceGarden.String(),
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"title":       structpb.NewStringValue("The Lord of the Ice Garden"),
						"description": structpb.NewStringValue("The Lord of the Ice Garden (Polish: Pan Lodowego Ogrodu) is a four-volume science fiction and fantasy novel by Polish writer Jaroslaw Grzedowicz."),
					},
				},
				ObjectProperties: []*pb.ObjectProperties{{
					PropName: "meta",
					Value: &pb.ObjectPropertiesValue{
						NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"isbn": structpb.NewStringValue("978-8374812962")}},
						ObjectProperties: []*pb.ObjectProperties{{
							PropName: "obj",
							Value:    &pb.ObjectPropertiesValue{NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue("some text")}}},
						}},
						ObjectArrayProperties: []*pb.ObjectArrayProperties{{
							PropName: "objs",
							Values: []*pb.ObjectPropertiesValue{{
								NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{"text": structpb.NewStringValue("some text")}},
							}},
						}},
					},
				}},
				ObjectArrayProperties: []*pb.ObjectArrayProperties{{
					PropName: "reviews",
					Values:   []*pb.ObjectPropertiesValue{{TextArrayProperties: []*pb.TextArrayProperties{{PropName: "tags", Values: []string{scifi, "fantasy"}}}}},
				}},
			},
		},
	}
}
