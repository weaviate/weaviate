package types_test

import (
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/cluster/router/types"
)

func TestReadReplicaSet_Shards(t *testing.T) {
	tests := []struct {
		name     string
		replicas []types.Replica
		want     []string
	}{
		{
			name:     "empty replicas",
			replicas: []types.Replica{},
			want:     []string{},
		},
		{
			name: "single replica",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple replicas different shards",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_C", NodeName: "node3", HostAddr: "host3"},
			},
			want: []string{"shard_A", "shard_B", "shard_C"},
		},
		{
			name: "multiple replicas same shard - should deduplicate",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_A", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_A", NodeName: "node3", HostAddr: "host3"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "mixed - multiple shards with duplicates",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_A", NodeName: "node3", HostAddr: "host3"}, // duplicate
				{ShardName: "shard_C", NodeName: "node4", HostAddr: "host4"},
				{ShardName: "shard_B", NodeName: "node5", HostAddr: "host5"}, // duplicate
			},
			want: []string{"shard_A", "shard_B", "shard_C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := types.ReadReplicaSet{Replicas: tt.replicas}
			got := rs.Shards()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadReplicaSet.Shards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteReplicaSet_Shards(t *testing.T) {
	tests := []struct {
		name     string
		replicas []types.Replica
		want     []string
	}{
		{
			name:     "empty replicas",
			replicas: []types.Replica{},
			want:     []string{},
		},
		{
			name: "single replica",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple replicas same shard - should deduplicate",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_A", NodeName: "node2", HostAddr: "host2"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple different shards",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
			},
			want: []string{"shard_A", "shard_B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := types.WriteReplicaSet{Replicas: tt.replicas}
			got := ws.Shards()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WriteReplicaSet.Shards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteReplicaSet_AdditionalShards(t *testing.T) {
	tests := []struct {
		name               string
		additionalReplicas []types.Replica
		want               []string
	}{
		{
			name:               "empty additional replicas",
			additionalReplicas: []types.Replica{},
			want:               []string{},
		},
		{
			name: "single additional replica",
			additionalReplicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple additional replicas same shard - should deduplicate",
			additionalReplicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_A", NodeName: "node2", HostAddr: "host2"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple different additional shards",
			additionalReplicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
			},
			want: []string{"shard_A", "shard_B"},
		},
		{
			name: "complex additional replica scenario",
			additionalReplicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_A", NodeName: "node3", HostAddr: "host3"}, // duplicate
				{ShardName: "shard_C", NodeName: "node4", HostAddr: "host4"},
			},
			want: []string{"shard_A", "shard_B", "shard_C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := types.WriteReplicaSet{AdditionalReplicas: tt.additionalReplicas}
			got := ws.AdditionalShards()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WriteReplicaSet.AdditionalShards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadReplicaSet_OtherMethods(t *testing.T) {
	replicas := []types.Replica{
		{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1:8080"},
		{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2:8080"},
	}
	rs := types.ReadReplicaSet{Replicas: replicas}

	t.Run("NodeNames", func(t *testing.T) {
		want := []string{"node1", "node2"}
		got := rs.NodeNames()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("NodeNames() = %v, want %v", got, want)
		}
	})

	t.Run("HostAddresses", func(t *testing.T) {
		want := []string{"host1:8080", "host2:8080"}
		got := rs.HostAddresses()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("HostAddresses() = %v, want %v", got, want)
		}
	})

	t.Run("EmptyReplicas", func(t *testing.T) {
		if rs.EmptyReplicas() {
			t.Error("EmptyReplicas() should return false for non-empty replica set")
		}

		emptyRS := types.ReadReplicaSet{Replicas: []types.Replica{}}
		if !emptyRS.EmptyReplicas() {
			t.Error("EmptyReplicas() should return true for empty replica set")
		}
	})
}

func TestWriteReplicaSet_OtherMethods(t *testing.T) {
	replicas := []types.Replica{
		{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1:8080"},
		{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2:8080"},
	}
	additionalReplicas := []types.Replica{
		{ShardName: "shard_C", NodeName: "node3", HostAddr: "host3:8080"},
	}
	ws := types.WriteReplicaSet{
		Replicas:           replicas,
		AdditionalReplicas: additionalReplicas,
	}

	t.Run("NodeNames", func(t *testing.T) {
		want := []string{"node1", "node2"}
		got := ws.NodeNames()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("NodeNames() = %v, want %v", got, want)
		}
	})

	t.Run("AdditionalNodeNames", func(t *testing.T) {
		want := []string{"node3"}
		got := ws.AdditionalNodeNames()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("AdditionalNodeNames() = %v, want %v", got, want)
		}
	})

	t.Run("HostAddresses", func(t *testing.T) {
		want := []string{"host1:8080", "host2:8080"}
		got := ws.HostAddresses()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("HostAddresses() = %v, want %v", got, want)
		}
	})

	t.Run("AdditionalHostAddresses", func(t *testing.T) {
		want := []string{"host3:8080"}
		got := ws.AdditionalHostAddresses()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("AdditionalHostAddresses() = %v, want %v", got, want)
		}
	})

	t.Run("IsEmpty", func(t *testing.T) {
		if ws.IsEmpty() {
			t.Error("IsEmpty() should return false for non-empty replica set")
		}

		emptyWS := types.WriteReplicaSet{Replicas: []types.Replica{}}
		if !emptyWS.IsEmpty() {
			t.Error("IsEmpty() should return true for empty replica set")
		}
	})

	t.Run("EmptyAdditionalReplicas", func(t *testing.T) {
		if ws.EmptyAdditionalReplicas() {
			t.Error("EmptyAdditionalReplicas() should return false when additional replicas exist")
		}

		wsNoAdditional := types.WriteReplicaSet{
			Replicas:           replicas,
			AdditionalReplicas: []types.Replica{},
		}
		if !wsNoAdditional.EmptyAdditionalReplicas() {
			t.Error("EmptyAdditionalReplicas() should return true when no additional replicas")
		}
	})
}
