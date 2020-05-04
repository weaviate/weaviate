//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"errors"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type Validator struct {
	schema  schema.Schema
	errors  *errorCompounder
	subject models.Classification
}

func NewValidator(sg schemaUC.SchemaGetter, subject models.Classification) *Validator {
	schema := sg.GetSchemaSkipAuth()
	return &Validator{
		schema:  schema,
		errors:  &errorCompounder{},
		subject: subject,
	}
}

func (v *Validator) Do() error {
	v.validate()

	err := v.errors.toError()
	if err != nil {
		return fmt.Errorf("invalid classification: %v", err)
	}

	return nil
}

func (v *Validator) validate() {
	if v.subject.Class == "" {
		v.errors.add(fmt.Errorf("class must be set"))
		return
	}

	class := v.schema.FindClassByName(schema.ClassName(v.subject.Class))
	if class == nil {
		v.errors.addf("class '%s' not found in schema", v.subject.Class)
		return
	}

	v.contextualTypeFeasibility()
	v.knnTypeFeasibility()
	v.basedOnProperties(class)
	v.classifyProperties(class)
}

func (v *Validator) contextualTypeFeasibility() {
	if !v.typeContextual() {
		return
	}

	if v.subject.K != nil {
		v.errors.addf("field 'k' can only be set for type 'knn', but got type 'contextual'")
	}

	if v.subject.TrainingSetWhere != nil {
		v.errors.addf("type is 'contextual', but 'trainingSetWhere' filter is set, for 'contextual' there is no training data, instead limit possible target data directly through setting 'targetWhere'")
	}
}

func (v *Validator) knnTypeFeasibility() {
	if !v.typeKNN() {
		return
	}

	if v.subject.TargetWhere != nil {
		v.errors.addf("type is 'knn', but 'targetWhere' filter is set, for 'knn' you cannot limit target data directly, instead limit training data through setting 'trainingSetWhere'")
	}
}

func (v *Validator) basedOnProperties(class *models.Class) {
	if v.subject.BasedOnProperties == nil || len(v.subject.BasedOnProperties) == 0 {
		v.errors.addf("basedOnProperties must have at least one property")
		return
	}

	if len(v.subject.BasedOnProperties) > 1 {
		v.errors.addf("only a single property in basedOnProperties supported at the moment, got %v",
			v.subject.BasedOnProperties)
		return
	}

	for _, prop := range v.subject.BasedOnProperties {
		v.basedOnProperty(class, prop)
	}
}

func (v *Validator) basedOnProperty(class *models.Class, propName string) {
	prop, ok := v.propertyByName(class, propName)
	if !ok {
		v.errors.addf("basedOnProperties: property '%s' does not exist", propName)
		return
	}

	dt, err := v.schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		v.errors.addf("basedOnProperties: %v", err)
		return
	}

	if !dt.IsPrimitive() {
		v.errors.addf("basedOnProperties: property '%s' must be of type 'text'", propName)
		return
	}

	if dt.AsPrimitive() != schema.DataTypeText {
		v.errors.addf("basedOnProperties: property '%s' must be of type 'text'", propName)
		return
	}
}

func (v *Validator) classifyProperties(class *models.Class) {
	if v.subject.ClassifyProperties == nil || len(v.subject.ClassifyProperties) == 0 {
		v.errors.add(fmt.Errorf("classifyProperties must have at least one property"))
		return
	}

	for _, prop := range v.subject.ClassifyProperties {
		v.classifyProperty(class, prop)
	}
}

func (v *Validator) classifyProperty(class *models.Class, propName string) {
	prop, ok := v.propertyByName(class, propName)
	if !ok {
		v.errors.addf("classifyProperties: property '%s' does not exist", propName)
		return
	}

	dt, err := v.schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		v.errors.addf("classifyProperties: %v", err)
		return
	}

	if dt.IsPrimitive() {
		v.errors.addf("classifyProperties: property '%s' must be of reference type (cref)", propName)
		return
	}

	if c := schema.CardinalityOfProperty(prop); c == schema.CardinalityMany {
		v.errors.addf("classifyProperties: property '%s'"+
			" is of cardinality 'many', can only classifiy references of cardinality 'atMostOne'", propName)
		return
	}

	if v.typeContextual() {
		if len(dt.Classes()) > 1 {
			v.errors.addf("classifyProperties: property '%s'"+
				" has more than one target class, classification of type 'contextual' requires exactly one target class", propName)
			return
		}
	}
}

func (v *Validator) propertyByName(class *models.Class, propName string) (*models.Property, bool) {
	for _, prop := range class.Properties {
		if prop.Name == propName {
			return prop, true
		}
	}

	return nil, false
}

func (v *Validator) typeContextual() bool {
	if v.subject.Type == nil {
		return false
	}

	return *v.subject.Type == "contextual"
}

func (v *Validator) typeKNN() bool {
	if v.subject.Type == nil {
		return true
	}

	return *v.subject.Type == "knn"
}

type errorCompounder struct {
	errors []error
}

func (ec *errorCompounder) addf(msg string, args ...interface{}) {
	ec.errors = append(ec.errors, fmt.Errorf(msg, args...))
}

func (ec *errorCompounder) add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *errorCompounder) toError() error {
	if len(ec.errors) == 0 {
		return nil
	}

	var msg strings.Builder
	for i, err := range ec.errors {
		if i != 0 {
			msg.WriteString(", ")
		}

		msg.WriteString(err.Error())
	}

	return errors.New(msg.String())
}
