package errorcompounder

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type ErrorCompounder struct {
	errors []error
}

func (ec *ErrorCompounder) Add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *ErrorCompounder) Addf(msg string, args ...interface{}) {
	ec.errors = append(ec.errors, fmt.Errorf(msg, args...))
}

func (ec *ErrorCompounder) AddWrap(err error, wrapMsg ...string) {
	if err != nil {
		ec.errors = append(ec.errors, errors.Wrap(err, wrapMsg[0]))
	}
}

func (ec *ErrorCompounder) ToError() error {
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
