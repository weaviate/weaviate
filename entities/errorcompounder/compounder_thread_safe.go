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
	"sync"
)

func NewSafe() *errorCompounderSafe {
	return &errorCompounderSafe{
		c: New(),
		m: new(sync.Mutex),
	}
}

type errorCompounderSafe struct {
	c *errorCompounder
	m *sync.Mutex
}

func (ec *errorCompounderSafe) Add(err error) {
	if err != nil {
		ec.m.Lock()
		defer ec.m.Unlock()

		ec.c.add(err)
	}
}

func (ec *errorCompounderSafe) Addf(format string, a ...any) {
	ec.m.Lock()
	defer ec.m.Unlock()

	ec.c.Addf(format, a...)
}

func (ec *errorCompounderSafe) AddWrapf(err error, format string, a ...any) {
	if err != nil {
		ec.m.Lock()
		defer ec.m.Unlock()

		ec.c.addWrapf(err, format, a...)
	}
}

func (ec *errorCompounderSafe) AddGroups(err error, groups ...string) {
	if err != nil {
		ec.m.Lock()
		defer ec.m.Unlock()

		ec.c.addGroups(err, groups...)
	}
}

func (ec *errorCompounderSafe) First() error {
	ec.m.Lock()
	defer ec.m.Unlock()

	return ec.c.First()
}

func (ec *errorCompounderSafe) ToError() error {
	ec.m.Lock()
	defer ec.m.Unlock()

	return ec.c.ToError()
}

func (ec *errorCompounderSafe) Len() int {
	ec.m.Lock()
	defer ec.m.Unlock()

	return ec.c.Len()
}

func (ec *errorCompounderSafe) Empty() bool {
	ec.m.Lock()
	defer ec.m.Unlock()

	return ec.c.Empty()
}
