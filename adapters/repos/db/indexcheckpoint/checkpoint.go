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

package indexcheckpoint

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var checkpointBucket = []byte("checkpoint")

// Checkpoints keeps track of the last indexed vector id for each shard.
// It stores the ids in a BoltDB file.
type Checkpoints struct {
	db *bolt.DB
}

func New(dir string, logger logrus.FieldLogger) (*Checkpoints, error) {
	path := filepath.Join(dir, "index.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "open %q", path)
	}

	ic := Checkpoints{
		db: db,
	}

	err = ic.initDB()
	if err != nil {
		return nil, err
	}

	return &ic, nil
}

func (c *Checkpoints) initDB() error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(checkpointBucket)
		return err
	})

	return errors.Wrap(err, "init db")
}

// Close the underlying DB
func (c *Checkpoints) Close() {
	c.db.Close()
}

func (c *Checkpoints) getID(shardID, targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("%s_%s", shardID, targetVector)
	}
	return shardID
}

func (c *Checkpoints) Get(shardID, targetVector string) (uint64, error) {
	var count uint64
	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(checkpointBucket)
		v := b.Get([]byte(c.getID(shardID, targetVector)))
		if v == nil {
			return nil
		}

		count = binary.LittleEndian.Uint64(v)

		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err, "get checkpoint")
	}

	return count, nil
}

func (c *Checkpoints) Update(shardID, targetVector string, id uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, id)

	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(checkpointBucket)
		return b.Put([]byte(c.getID(shardID, targetVector)), buf)
	})
	if err != nil {
		return errors.Wrap(err, "update checkpoint")
	}

	return nil
}

func (c *Checkpoints) Delete(shardID, targetVector string) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(checkpointBucket)
		return b.Delete([]byte(c.getID(shardID, targetVector)))
	})
	if err != nil {
		return errors.Wrap(err, "delete checkpoint")
	}

	return nil
}

func (c *Checkpoints) Drop() error {
	c.db.Close()
	return os.Remove(c.Filename())
}

func (c *Checkpoints) Filename() string {
	return c.db.Path()
}
