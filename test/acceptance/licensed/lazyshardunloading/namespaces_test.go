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

package lazyshardunloading

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func newAuthClient(uri, key string) *apiclient.Weaviate {
	transport := httptransport.New(uri, "/v1", []string{"http"})
	transport.DefaultAuthentication = httptransport.BearerToken(key)
	return apiclient.New(transport, strfmt.Default)
}

// Not parallel: the namespace helpers share the package-global test client.
func TestNamespaceSuspendResumeWhileUnloading(t *testing.T) {
	if strings.TrimSpace(os.Getenv(docker.LicenseKeyEnv)) == "" {
		t.Setenv(docker.LicenseKeyEnv, docker.TestWeaviateLicenseKey)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const (
		adminUser = "admin-user"
		adminKey  = "admin-key"
		namespace = "tenantco"
		limit     = 3
	)
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(knobs{"max_loaded_shards": "3"})).
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithDbUsers().
		WithRBAC().
		WithRbacRoots(adminUser).
		WithNamespaces())
	node := compose.GetWeaviate()
	helper.SetupClient(node.URI())
	t.Cleanup(helper.ResetClient)
	waitLimit(ctx, t, compose, 1, limit)

	helper.CreateNamespace(t, namespace, adminKey)
	userKey := helper.CreateUserWithNamespace(t, "u1", namespace, adminKey)
	helper.AssignRoleToUser(t, adminKey, "admin", namespace+":u1")
	cl := newAuthClient(node.URI(), userKey)

	const class = "Docs"
	tenants := tenantNames("t", 15)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	atLimit := func(n int) bool { return n <= limit }
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, atLimit, "the namespace's shards drain to the limit")

	wctx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		for wctx.Err() == nil {
			if err := writeObjects(wctx, cl, l, class, tenants[r.Intn(len(tenants))], 1, nil, r); err != nil {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	for range 3 {
		helper.SuspendNamespace(t, namespace, adminKey)
		helper.ResumeNamespace(t, namespace, adminKey)
	}
	stopWriter()
	wg.Wait()

	require.NoError(t, randomWorkload(ctx, cl, l, class, tenants, 300, 4), "every request succeeds after the resume")
	verifyAll(ctx, t, cl, l, class, tenants)
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, atLimit, "the resumed namespace drains to the limit")
}
