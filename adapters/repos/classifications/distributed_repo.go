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

package classifications

import (
	"context"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/cluster"
)

const DefaultTxTTL = 60 * time.Second

type DistributedRepo struct {
	sync.RWMutex
	txRemote  *cluster.TxManager
	localRepo localRepo
}

type localRepo interface {
	Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error)
	Put(ctx context.Context, classification models.Classification) error
}

func NewDistributeRepo(remoteClient cluster.Client,
	memberLister cluster.MemberLister, localRepo localRepo,
	logger logrus.FieldLogger,
) *DistributedRepo {
	broadcaster := cluster.NewTxBroadcaster(memberLister, remoteClient)
	txRemote := cluster.NewTxManager(broadcaster, &dummyTxPersistence{}, logger)
	txRemote.StartAcceptIncoming()
	repo := &DistributedRepo{
		txRemote:  txRemote,
		localRepo: localRepo,
	}

	repo.txRemote.SetCommitFn(repo.incomingCommit)

	return repo
}

func (r *DistributedRepo) Get(ctx context.Context,
	id strfmt.UUID,
) (*models.Classification, error) {
	r.RLock()
	defer r.RUnlock()

	return r.localRepo.Get(ctx, id)
}

func (r *DistributedRepo) Put(ctx context.Context,
	pl models.Classification,
) error {
	r.Lock()
	defer r.Unlock()

	tx, err := r.txRemote.BeginTransaction(ctx, classification.TransactionPut,
		classification.TransactionPutPayload{
			Classification: pl,
		}, DefaultTxTTL)
	if err != nil {
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	err = r.txRemote.CommitWriteTransaction(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return r.localRepo.Put(ctx, pl)
}

func (r *DistributedRepo) incomingCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	if tx.Type != classification.TransactionPut {
		return errors.Errorf("unrecognized tx type: %s", tx.Type)
	}

	return r.localRepo.Put(ctx, tx.Payload.(classification.TransactionPutPayload).
		Classification)
}

func (r *DistributedRepo) TxManager() *cluster.TxManager {
	return r.txRemote
}

// NOTE: classifications do not yet make use of the new durability guarantees
// introduced by the txManager as part of v1.21.3. The reasoning behind this is
// that the classification itself is not crash-safe anyway, so there is no
// point. We need to decide down the line what to do with this? It is a rarely
// used, but not used feature. For now we are not aware of anyone having any
// issues with its stability.
type dummyTxPersistence struct{}

func (d *dummyTxPersistence) StoreTx(ctx context.Context, tx *cluster.Transaction) error {
	return nil
}

func (d *dummyTxPersistence) DeleteTx(ctx context.Context, txID string) error {
	return nil
}

func (d *dummyTxPersistence) IterateAll(ctx context.Context, cb func(tx *cluster.Transaction)) error {
	return nil
}
