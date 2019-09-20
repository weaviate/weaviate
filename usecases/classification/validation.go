package classification

import (
	"errors"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

func Validate(in models.Classification) error {
	ec := &errorCompounder{}

	if in.Class == "" {
		ec.add(fmt.Errorf("class must be set"))
	}

	if in.BasedOnProperties == nil || len(in.BasedOnProperties) == 0 {
		ec.add(fmt.Errorf("basedOnProperties must have at least one property"))
	}

	if len(in.BasedOnProperties) > 1 {
		ec.add(fmt.Errorf("only a single property in basedOnProperties supported at the moment, got %v",
			in.BasedOnProperties))
	}

	if in.ClassifyProperties == nil || len(in.ClassifyProperties) == 0 {
		ec.add(fmt.Errorf("classifyProperties must have at least one property"))
	}

	err := ec.toError()
	if err != nil {
		return fmt.Errorf("invalid classification: %v", err)
	}

	return nil
}

type errorCompounder struct {
	errors []error
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
