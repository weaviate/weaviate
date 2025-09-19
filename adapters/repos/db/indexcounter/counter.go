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

package indexcounter

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/weaviate/weaviate/entities/diskio"

	"github.com/pkg/errors"
)

type Counter struct {
	count uint64
	sync.Mutex
	f *os.File
	w diskio.WriterSeekerCloser
}

func New(shardPath string) (cr *Counter, rerr error) {
	fileName := fmt.Sprintf("%s/indexcount", shardPath)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	// The lifetime of the `f` exceeds this constructor as we store the open file for later use in Counter.
	// invariant: We close `f`  **only** if any error happened after successfully opening the file. To avoid leaking open file descriptor.
	// NOTE: This `defer` works even with `err` being shadowed in the whole function because defer checks for named `rerr` return value.
	defer func() {
		if rerr != nil {
			f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	var initialCount uint64 = 0
	if stat.Size() > 0 {
		// the file has existed before, we need to initialize with its content
		err := binary.Read(f, binary.LittleEndian, &initialCount)
		if err != nil {
			return nil, errors.Wrap(err, "read initial count from file")
		}

	}

	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"strategy":  "",
		"operation": "counter",
	})

	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	meteredW := diskio.NewMeteredWriter(f, writeCB)

	return &Counter{
		count: initialCount,
		f:     f,
		w:     meteredW,
	}, nil
}

func (c *Counter) Get() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.count
}

func (c *Counter) GetAndInc() (uint64, error) {
	c.Lock()
	defer c.Unlock()
	before := c.count
	c.count++

	c.w.Seek(0, 0)
	err := binary.Write(c.w, binary.LittleEndian, &c.count)
	if err != nil {
		return 0, errors.Wrap(err, "increase counter on disk")
	}
	c.w.Seek(0, 0)
	return before, nil
}

// PreviewNext can be used to check if there is data present in the index, if
// it returns 0, you can be certain that no data exists
func (c *Counter) PreviewNext() uint64 {
	c.Lock()
	defer c.Unlock()

	return c.count
}

func (c *Counter) Drop() error {
	c.Lock()
	defer c.Unlock()
	if c.f == nil {
		return nil
	}
	filename := c.FileName()
	c.f.Close()
	err := diskio.Remove(filename, "counter")
	if err != nil {
		return errors.Wrap(err, "drop counter file")
	}
	return nil
}

func (c *Counter) FileName() string {
	return c.f.Name()
}
