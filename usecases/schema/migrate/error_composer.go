package migrate

import (
	"errors"
	"fmt"
	"strings"
)

type errorComposer struct {
	errors []error
}

func newErrorComposer() *errorComposer {
	return &errorComposer{}
}

func (e *errorComposer) Add(err error) {
	if err != nil {
		fmt.Printf("error is %v", err)
		e.errors = append(e.errors, err)
	}
}

func (e *errorComposer) Compose() error {
	if len(e.errors) == 0 {
		return nil
	}

	var s strings.Builder
	s.WriteString("migrator composer: ")

	for i, err := range e.errors {
		if i > 0 {
			s.WriteString(", ")
		}

		s.WriteString(err.Error())
	}

	return errors.New(s.String())
}
