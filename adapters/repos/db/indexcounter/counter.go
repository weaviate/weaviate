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

package indexcounter

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type Counter struct {
	count uint32
	sync.Mutex
	f *os.File
}

func New(shardID string, rootPath string) (*Counter, error) {
	fileName := fmt.Sprintf("%s/%s.indexcount", rootPath, shardID)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	var initialCount uint32 = 0
	if stat.Size() > 0 {
		// the file has existed before, we need to initialize with its content
		err := binary.Read(f, binary.LittleEndian, &initialCount)
		if err != nil {
			return nil, errors.Wrap(err, "read initial count from file")
		}
	}

	return &Counter{
		count: initialCount,
		f:     f,
	}, nil
}

func (c *Counter) GetAndInc() (uint32, error) {
	c.Lock()
	defer c.Unlock()
	before := c.count
	c.count++
	c.f.Seek(0, 0)
	err := binary.Write(c.f, binary.LittleEndian, &c.count)
	if err != nil {
		return 0, errors.Wrap(err, "increase counter on disk")
	}
	c.f.Seek(0, 0)
	return before, nil
}
