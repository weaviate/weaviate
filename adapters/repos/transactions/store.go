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

package txstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/cluster"
	"go.etcd.io/bbolt"
)

var txBucket = []byte("transactions")

type Store struct {
	db           *bbolt.DB
	log          logrus.FieldLogger
	homeDir      string
	unmarshaller unmarshalFn
}

func NewStore(homeDir string, logger logrus.FieldLogger) *Store {
	return &Store{
		homeDir: homeDir,
		log:     logger,
	}
}

func (s *Store) SetUmarshalFn(fn unmarshalFn) {
	s.unmarshaller = fn
}

func (s *Store) Open() error {
	if err := os.MkdirAll(s.homeDir, 0o777); err != nil {
		return fmt.Errorf("create root directory %q: %w", s.homeDir, err)
	}

	path := path.Join(s.homeDir, "tx.db")
	boltDB, err := initBoltDB(path)
	if err != nil {
		return fmt.Errorf("init bolt_db: %w", err)
	}

	s.db = boltDB

	return nil
}

func (s *Store) StoreTx(ctx context.Context, tx *cluster.Transaction) error {
	data, err := json.Marshal(txWrapper{
		ID:      tx.ID,
		Payload: tx.Payload,
		Type:    tx.Type,
	})
	if err != nil {
		return fmt.Errorf("marshal tx: %w", err)
	}

	return s.db.Update(func(boltTx *bbolt.Tx) error {
		b := boltTx.Bucket(txBucket)
		return b.Put([]byte(tx.ID), data)
	})
}

func (s *Store) DeleteTx(ctx context.Context, txId string) error {
	return s.db.Update(func(boltTx *bbolt.Tx) error {
		b := boltTx.Bucket(txBucket)
		return b.Delete([]byte(txId))
	})
}

func (s *Store) IterateAll(ctx context.Context,
	cb func(tx *cluster.Transaction),
) error {
	return s.db.View(func(boltTx *bbolt.Tx) error {
		b := boltTx.Bucket(txBucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var txWrap txWrapperRead
			if err := json.Unmarshal(v, &txWrap); err != nil {
				return err
			}

			tx := cluster.Transaction{
				ID:   txWrap.ID,
				Type: txWrap.Type,
			}

			pl, err := s.unmarshaller(tx.Type, txWrap.Payload)
			if err != nil {
				return err
			}

			tx.Payload = pl

			cb(&tx)

		}
		return nil
	})
}

func (s *Store) Close() error {
	return nil
}

func initBoltDB(filePath string) (*bbolt.DB, error) {
	db, err := bbolt.Open(filePath, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", filePath, err)
	}

	root := func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(txBucket)
		return err
	}

	return db, db.Update(root)
}

type txWrapper struct {
	ID      string                  `json:"id"`
	Payload any                     `json:"payload"`
	Type    cluster.TransactionType `json:"type"`
}

// delayed unmarshalling of the payload, so we can inject a specific
// marshaller
type txWrapperRead struct {
	ID      string                  `json:"id"`
	Payload json.RawMessage         `json:"payload"`
	Type    cluster.TransactionType `json:"type"`
}

type unmarshalFn func(txType cluster.TransactionType, payload json.RawMessage) (any, error)
