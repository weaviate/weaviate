//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errorcompounder

import (
	"fmt"
	"maps"
	"math"
	"slices"
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
	ToErrorLimited(limit int) error
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
	return ec.toError(math.MaxInt)
}

func (ec *errorCompounder) ToErrorLimited(limit int) error {
	return ec.toError(max(limit, 1))
}

func (ec *errorCompounder) toError(limit int) error {
	if ec.Empty() {
		return nil
	}

	errs := make([]error, 0, ec.Len())
	collect(ec.top, &errs)

	var b strings.Builder
	rendered := render(ec.top, &b, limit)

	// without the count a truncated message reads like the complete list
	if omitted := ec.Len() - rendered; omitted > 0 {
		fmt.Fprintf(&b, " (and %d more)", omitted)
	}
	return &compoundError{msg: b.String(), errs: errs}
}

// collect appends every error under e. A caller classifying a compound reads all
// of them, so no limit applies here.
func collect(e *entry, errs *[]error) {
	*errs = append(*errs, e.errors...)
	for _, name := range slices.Sorted(maps.Keys(e.groups)) {
		collect(e.groups[name], errs)
	}
}

// render writes at most limit errors into b and reports how many it wrote. The
// groups are walked in name order: a limit decides which errors reach the
// message, so ranging the map would name a different set on every render.
func render(e *entry, b *strings.Builder, limit int) int {
	rendered := 0
	addComma := false
	write := func(s string) {
		if addComma {
			b.WriteString(", ")
		}
		b.WriteString(s)
		addComma = true
	}

	for _, err := range e.errors {
		if rendered == limit {
			return rendered
		}
		write(err.Error())
		rendered++
	}
	for _, name := range slices.Sorted(maps.Keys(e.groups)) {
		if rendered == limit {
			return rendered
		}
		write("\"" + name + "\": {")
		rendered += render(e.groups[name], b, limit-rendered)
		b.WriteString("}")
	}
	return rendered
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

// compoundError renders every collected error into a single message while
// keeping those errors reachable for errors.Is and errors.As. A limited error
// renders only part of them into the message; all of them stay reachable.
type compoundError struct {
	msg  string
	errs []error
}

func (e *compoundError) Error() string {
	return e.msg
}

func (e *compoundError) Unwrap() []error {
	return e.errs
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

// GroupNames renders each var as a group name. A group is named by a string, so
// a var of another type is formatted rather than dropped: dropping one would
// file the error under a path its caller did not ask for.
func GroupNames(vars ...interface{}) []string {
	groups := make([]string, 0, len(vars))
	for _, v := range vars {
		groups = append(groups, fmt.Sprint(v))
	}
	return groups
}
