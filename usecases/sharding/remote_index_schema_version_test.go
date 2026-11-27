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

package sharding

import (
	"context"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// versionRecordingClient records the schema version each read sends.
type versionRecordingClient struct {
	RemoteIndexClient
	mu  sync.Mutex
	got []uint64
}

func (c *versionRecordingClient) record(v uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, v)
}

func (c *versionRecordingClient) GetObject(_ context.Context, _, _, _ string, _ strfmt.UUID,
	_ search.SelectProperties, _ additional.Properties, v uint64,
) (*storobj.Object, error) {
	c.record(v)
	return nil, nil
}

func (c *versionRecordingClient) Exists(_ context.Context, _, _, _ string, _ strfmt.UUID, v uint64) (bool, error) {
	c.record(v)
	return false, nil
}

func (c *versionRecordingClient) MultiGetObjects(_ context.Context, _, _, _ string, _ []strfmt.UUID, v uint64,
) ([]*storobj.Object, error) {
	c.record(v)
	return nil, nil
}

func (c *versionRecordingClient) SearchShard(_ context.Context, _, _, _ string, _ []models.Vector, _ []string,
	_ float32, _ int, _ *filters.LocalFilter, _ *searchparams.KeywordRanking, _ []filters.Sort, _ *filters.Cursor,
	_ *searchparams.GroupBy, _ additional.Properties, _ *dto.TargetCombination, _ []string, v uint64,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	c.record(v)
	return nil, nil, nil, nil
}

func (c *versionRecordingClient) Aggregate(_ context.Context, _, _, _ string, _ aggregation.Params, v uint64,
) (*aggregation.Result, error) {
	c.record(v)
	return nil, nil
}

func (c *versionRecordingClient) FindUUIDs(_ context.Context, _, _, _ string, _ *filters.LocalFilter, _ int, v uint64,
) ([]strfmt.UUID, error) {
	c.record(v)
	return nil, nil
}

type onNode0 struct{}

func (onNode0) ShardOwner(string, string) (string, error)      { return "N0", nil }
func (onNode0) ShardReplicas(string, string) ([]string, error) { return []string{"N0"}, nil }

// TestRemoteIndexReadsSendTheLocalClassVersion checks every shard read sends
// the class version this node holds, so the receiving node can wait for it.
func TestRemoteIndexReadsSendTheLocalClassVersion(t *testing.T) {
	tests := []struct {
		name     string
		versions func() uint64
		want     uint64
	}{
		{name: "the local class version", versions: func() uint64 { return 7 }, want: 7},
		{name: "no version reader sends no version", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			logger, _ := test.NewNullLogger()
			client := &versionRecordingClient{}
			ri := NewRemoteIndex("C", onNode0{}, tt.versions, &fakeNodeResolver{map[string]string{"N0": "H0"}}, client)

			_, err := ri.GetObject(ctx, "S", "", nil, additional.Properties{})
			require.NoError(t, err)
			_, err = ri.Exists(ctx, "S", "")
			require.NoError(t, err)
			_, err = ri.MultiGetObjects(ctx, "S", nil)
			require.NoError(t, err)
			_, _, _, _, err = ri.SearchShard(ctx, "S", nil, nil, 0, 1, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
			require.NoError(t, err)
			_, err = ri.SearchAllReplicas(ctx, logger, "S", nil, nil, 0, 1, nil, nil, nil, nil, nil, additional.Properties{}, "N1", nil, nil)
			require.NoError(t, err)
			_, err = ri.Aggregate(ctx, "N0", "S", aggregation.Params{})
			require.NoError(t, err)
			_, err = ri.FindUUIDs(ctx, "S", nil, 1)
			require.NoError(t, err)

			require.Equal(t, []uint64{tt.want, tt.want, tt.want, tt.want, tt.want, tt.want, tt.want}, client.got)
		})
	}
}

// readIndex answers every incoming read with an empty result.
type readIndex struct {
	RemoteIndexIncomingRepo
}

func (readIndex) IncomingGetObject(context.Context, string, strfmt.UUID, search.SelectProperties,
	additional.Properties,
) (*storobj.Object, error) {
	return nil, nil
}

func (readIndex) IncomingExists(context.Context, string, strfmt.UUID) (bool, error) { return true, nil }

func (readIndex) IncomingMultiGetObjects(context.Context, string, []strfmt.UUID) ([]*storobj.Object, error) {
	return nil, nil
}

func (readIndex) IncomingSearch(context.Context, string, []models.Vector, []string, float32, int,
	*filters.LocalFilter, *searchparams.KeywordRanking, []filters.Sort, *filters.Cursor, *searchparams.GroupBy,
	additional.Properties, *dto.TargetCombination, []string,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	return nil, nil, nil, nil
}

func (readIndex) IncomingAggregate(context.Context, string, aggregation.Params, interface{}) (*aggregation.Result, error) {
	return nil, nil
}

func (readIndex) IncomingFindUUIDs(context.Context, string, *filters.LocalFilter, int) ([]strfmt.UUID, error) {
	return nil, nil
}

// incomingRepo answers the version-aware lookup with index, and records the
// class version each read asked with.
type incomingRepo struct {
	index RemoteIndexIncomingRepo
	asked []uint64
}

func (r *incomingRepo) GetIndexForIncomingSharding(context.Context, schema.ClassName) RemoteIndexIncomingRepo {
	return r.index
}

func (r *incomingRepo) IndexForIncomingRead(_ context.Context, _ schema.ClassName, version uint64) RemoteIndexIncomingRepo {
	r.asked = append(r.asked, version)
	return r.index
}

// TestIncomingReadsAskWithTheCoordinatorsClassVersion checks every incoming read
// looks its index up with the coordinator's class version, serves the index it
// gets, and reports a missing index with the existing unprocessable error.
func TestIncomingReadsAskWithTheCoordinatorsClassVersion(t *testing.T) {
	reads := []struct {
		name string
		read func(ctx context.Context, rii *RemoteIndexIncoming, version uint64) error
	}{
		{"GetObject", func(ctx context.Context, rii *RemoteIndexIncoming, v uint64) error {
			_, err := rii.GetObject(ctx, "C", "S", "", nil, additional.Properties{}, v)
			return err
		}},
		{"Exists", func(ctx context.Context, rii *RemoteIndexIncoming, v uint64) error {
			_, err := rii.Exists(ctx, "C", "S", "", v)
			return err
		}},
		{"MultiGetObjects", func(ctx context.Context, rii *RemoteIndexIncoming, v uint64) error {
			_, err := rii.MultiGetObjects(ctx, "C", "S", nil, v)
			return err
		}},
		{"Search", func(ctx context.Context, rii *RemoteIndexIncoming, v uint64) error {
			_, _, _, err := rii.Search(ctx, "C", "S", nil, nil, 0, 1, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil, v)
			return err
		}},
		{"Aggregate", func(ctx context.Context, rii *RemoteIndexIncoming, v uint64) error {
			_, err := rii.Aggregate(ctx, "C", "S", aggregation.Params{}, v)
			return err
		}},
		{"FindUUIDs", func(ctx context.Context, rii *RemoteIndexIncoming, v uint64) error {
			_, err := rii.FindUUIDs(ctx, "C", "S", nil, 1, v)
			return err
		}},
	}

	cases := []struct {
		name    string
		index   RemoteIndexIncomingRepo
		wantErr bool
	}{
		{name: "an index answers", index: readIndex{}},
		{name: "no index is reported missing", wantErr: true},
	}

	for _, r := range reads {
		for _, c := range cases {
			t.Run(r.name+"/"+c.name, func(t *testing.T) {
				repo := &incomingRepo{index: c.index}
				rii := NewRemoteIndexIncoming(repo, nil, nil)

				err := r.read(t.Context(), rii, 7)

				require.Equal(t, []uint64{7}, repo.asked)
				if !c.wantErr {
					require.NoError(t, err)
					return
				}
				require.ErrorAs(t, err, &enterrors.ErrUnprocessable{})
				require.EqualError(t, err, `local index "C" not found`)
			})
		}
	}
}
