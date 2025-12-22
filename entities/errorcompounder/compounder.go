//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errorcompounder

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type ErrorCompounder interface {
	Add(err error)
	Addf(format string, a ...any)
	AddWrapf(err error, format string, a ...any)
	AddGroups(err error, groups ...string)

	Empty() bool
	Len() int

	First() error
	ToError() error
}

// ----------------------------------------------------------------------------

func New() *errorCompounder {
	return &errorCompounder{top: &entry{}}
}

type errorCompounder struct {
	top *entry
}

func (ec *errorCompounder) Add(err error) {
	if err != nil {
		ec.add(err)
	}
}

func (ec *errorCompounder) Addf(format string, a ...any) {
	ec.add(fmt.Errorf(format, a...))
}

func (ec *errorCompounder) AddWrapf(err error, format string, a ...any) {
	if err != nil {
		ec.addWrapf(err, format, a...)
	}
}

func (ec *errorCompounder) AddGroups(err error, groups ...string) {
	if err != nil {
		ec.addGroups(err, groups...)
	}
}

func (ec *errorCompounder) Len() int {
	return ec.top.len()
}

func (ec *errorCompounder) Empty() bool {
	return ec.top.empty()
}

func (ec *errorCompounder) First() error {
	return ec.top.first()
}

func (ec *errorCompounder) ToError() error {
	if ec.Empty() {
		return nil
	}

	var b strings.Builder

	var f func(*entry)
	f = func(e *entry) {
		addComma := false
		for _, err := range e.errors {
			if addComma {
				b.WriteString(", ")
			}
			b.WriteString(err.Error())
			addComma = true
		}
		for name, group := range e.groups {
			if addComma {
				b.WriteString(", ")
			}
			b.WriteString("\"")
			b.WriteString(name)
			b.WriteString("\": {")
			f(group)
			b.WriteString("}")
			addComma = true
		}
	}
	f(ec.top)
	return errors.New(b.String())
}

func (ec *errorCompounder) add(err error) {
	ec.top.errors = append(ec.top.errors, err)
}

func (ec *errorCompounder) addWrapf(err error, format string, a ...any) {
	ec.add(errors.Wrapf(err, format, a...))
}

func (ec *errorCompounder) addGroups(err error, groups ...string) {
	target := ec.top
	for _, name := range groups {
		if target.groups == nil {
			target.groups = map[string]*entry{}
		}
		group, ok := target.groups[name]
		if !ok {
			group = &entry{}
			target.groups[name] = group
		}
		target = group
	}
	target.errors = append(target.errors, err)
}

// ----------------------------------------------------------------------------

type entry struct {
	errors []error
	groups map[string]*entry
}

func (e *entry) empty() bool {
	if len(e.errors) > 0 {
		return false
	}
	for _, group := range e.groups {
		if !group.empty() {
			return false
		}
	}
	return true
}

func (e *entry) first() error {
	if len(e.errors) > 0 {
		return e.errors[0]
	}
	for _, nested := range e.groups {
		if err := nested.first(); err != nil {
			return err
		}
	}
	return nil
}

func (e *entry) len() int {
	ln := len(e.errors)
	for _, nested := range e.groups {
		ln += nested.len()
	}
	return ln
}
