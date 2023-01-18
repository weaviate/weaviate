//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	txRemote := cluster.NewTxManager(broadcaster, logger)
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
