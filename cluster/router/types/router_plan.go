package types

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

type RoutingPlanBuildOptions struct {
	Collection             string
	Shard                  string
	ConsistencyLevel       ConsistencyLevel
	DirectCandidateReplica string
}

func (r RoutingPlanBuildOptions) Validate() error {
	if r.Collection == "" {
		return fmt.Errorf("no collection specified for routing plan building")
	}
	if r.Shard == "" {
		return fmt.Errorf("no shard specified for routing plan building")
	}
	return nil
}

type RoutingPlan struct {
	Collection string
	Shard      string
	Replicas   []string

	ConsistencyLevel    ConsistencyLevel
	IntConsistencyLevel int
	ReplicasHostAddrs   []string
	AdditionalHostAddrs []string
}

func (r RoutingPlan) LogFields() logrus.Fields {
	return logrus.Fields{
		"collection":            r.Collection,
		"shard":                 r.Shard,
		"replicas":              r.Replicas,
		"consistency_level":     r.ConsistencyLevel,
		"replicas_host_addrs":   r.ReplicasHostAddrs,
		"additional_host_addrs": r.AdditionalHostAddrs,
	}
}

func (r RoutingPlan) ValidateConsistencyLevel() (int, error) {
	level := r.ConsistencyLevel.ToInt(len(r.Replicas))
	if n := len(r.ReplicasHostAddrs); level > n {
		return 0, fmt.Errorf("impossible to satisfy consistency level (%d) > available replicas (%d) replicas=%+q addrs=%+q", level, n, r.Replicas, r.ReplicasHostAddrs)
	}
	return level, nil
}
