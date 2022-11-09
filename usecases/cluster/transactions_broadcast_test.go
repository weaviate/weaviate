//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package cluster

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroadcastOpenTransaction(t *testing.T) {
	client := &fakeClient{}
	state := &fakeState{[]string{"host1", "host2", "host3"}}

	bc := NewTxBroadcaster(state, client)

	tx := &Transaction{ID: "foo"}

	err := bc.BroadcastTransaction(context.Background(), tx)
	require.Nil(t, err)

	assert.ElementsMatch(t, []string{"host1", "host2", "host3"}, client.openCalled)
}

func TestBroadcastOpenTransactionWithReturnPayload(t *testing.T) {
	client := &fakeClient{}
	state := &fakeState{[]string{"host1", "host2", "host3"}}

	bc := NewTxBroadcaster(state, client)
	bc.SetConsensusFunction(func(in []*Transaction) (*Transaction, error) {
		// instead of actually reaching a consensus this test mock simply merged
		// all the individual results. For testing purposes this is even better
		// because now we can be sure that every element was considered.
		merged := ""
		for _, tx := range in {
			if len(merged) > 0 {
				merged += ","
			}
			merged += tx.Payload.(string)
		}

		return &Transaction{
			Payload: merged,
		}, nil
	})

	tx := &Transaction{ID: "foo"}

	err := bc.BroadcastTransaction(context.Background(), tx)
	require.Nil(t, err)

	assert.ElementsMatch(t, []string{"host1", "host2", "host3"}, client.openCalled)

	results := strings.Split(tx.Payload.(string), ",")
	assert.ElementsMatch(t, []string{
		"hello_from_host1",
		"hello_from_host2",
		"hello_from_host3",
	}, results)
}

func TestBroadcastAbortTransaction(t *testing.T) {
	client := &fakeClient{}
	state := &fakeState{[]string{"host1", "host2", "host3"}}

	bc := NewTxBroadcaster(state, client)

	tx := &Transaction{ID: "foo"}

	err := bc.BroadcastAbortTransaction(context.Background(), tx)
	require.Nil(t, err)

	assert.ElementsMatch(t, []string{"host1", "host2", "host3"}, client.abortCalled)
}

func TestBroadcastCommitTransaction(t *testing.T) {
	client := &fakeClient{}
	state := &fakeState{[]string{"host1", "host2", "host3"}}

	bc := NewTxBroadcaster(state, client)

	tx := &Transaction{ID: "foo"}

	err := bc.BroadcastCommitTransaction(context.Background(), tx)
	require.Nil(t, err)

	assert.ElementsMatch(t, []string{"host1", "host2", "host3"}, client.commitCalled)
}

type fakeState struct {
	hosts []string
}

func (f *fakeState) Hostnames() []string {
	return f.hosts
}

type fakeClient struct {
	sync.Mutex
	openCalled   []string
	abortCalled  []string
	commitCalled []string
}

func (f *fakeClient) OpenTransaction(ctx context.Context, host string, tx *Transaction) error {
	f.Lock()
	defer f.Unlock()

	f.openCalled = append(f.openCalled, host)
	tx.Payload = "hello_from_" + host
	return nil
}

func (f *fakeClient) AbortTransaction(ctx context.Context, host string, tx *Transaction) error {
	f.Lock()
	defer f.Unlock()

	f.abortCalled = append(f.abortCalled, host)
	return nil
}

func (f *fakeClient) CommitTransaction(ctx context.Context, host string, tx *Transaction) error {
	f.Lock()
	defer f.Unlock()

	f.commitCalled = append(f.commitCalled, host)
	return nil
}
