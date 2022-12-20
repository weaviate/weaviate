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

package replica

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"golang.org/x/sync/errgroup"
)

type ConsistencyLevel string

const (
	One    ConsistencyLevel = "ONE"
	Quorum ConsistencyLevel = "QUORUM"
	All    ConsistencyLevel = "ALL"
)

func cLevel(l ConsistencyLevel, n int) int {
	switch l {
	case All:
		return n
	case Quorum:
		return n/2 + 1
	default:
		return 1
	}
}

// Finder finds replicated objects
type Finder struct {
	RClient       // needed to commit and abort operation
	replicaFinder // host names of replicas
	resolver      nodeResolver
	class         string
}

func NewFinder(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client RClient,
) *Finder {
	return &Finder{
		class:    className,
		resolver: nodeResolver,
		replicaFinder: &rFinder{
			schema:   stateGetter,
			resolver: nodeResolver,
			class:    className,
		},
		RClient: client,
	}
}

// FindOne finds one object which satisfies the giving consistency
func (f *Finder) FindOne(ctx context.Context, level ConsistencyLevel, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (*storobj.Object, error) {
	replicas := f.replicaFinder.FindReplicas(shard)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("%w : class %q shard %q", errReplicaNotFound, f.class, shard)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	writer := func() <-chan tuple {
		responses := make(chan tuple, len(replicas))
		var g errgroup.Group
		for i, host := range replicas {
			i, host := i, host
			g.Go(func() error {
				o, err := f.FindObject(ctx, host, f.class, shard, id, props, additional)
				responses <- tuple{o, i, err}
				return nil
			})
		}
		go func() { g.Wait(); close(responses) }()
		return responses
	}

	return readObject(writer(), cLevel(level, len(replicas)), replicas)
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context, nodeName, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.resolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	return f.RClient.FindObject(ctx, host, f.class, shard, id, props, additional)
}

func readObject(responses <-chan tuple, cl int, replicas []string) (*storobj.Object, error) {
	counters := make([]tuple, len(replicas))
	nnf := 0
	for r := range responses {
		if r.err != nil {
			counters[r.i] = tuple{nil, 0, r.err}
			continue
		} else if r.o == nil {
			nnf++
			continue
		}
		counters[r.i] = tuple{r.o, 1, nil}
		max := 0
		for i := range counters {
			if counters[i].o != nil && i != r.i && counters[i].o.LastUpdateTimeUnix() == r.o.LastUpdateTimeUnix() {
				counters[i].i++
			}
			if max < counters[i].i {
				max = counters[i].i
			}
			if max >= cl {
				return counters[i].o, nil
			}
		}
	}
	if nnf == len(replicas) { // object doesn't exist
		return nil, nil
	}

	var sb strings.Builder
	for i, c := range counters {
		if i != 0 {
			sb.WriteString(", ")
		}
		if c.err != nil {
			fmt.Fprintf(&sb, "%s: %s", replicas[i], c.err.Error())
		} else if c.o == nil {
			fmt.Fprintf(&sb, "%s: 0", replicas[i])
		} else {
			fmt.Fprintf(&sb, "%s: %d", replicas[i], c.o.LastUpdateTimeUnix())
		}
	}
	return nil, errors.New(sb.String())
}

type tuple struct {
	o   *storobj.Object
	i   int
	err error
}
