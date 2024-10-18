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

package metadata

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/exp/metadata/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testServerSetup gives you a ready to use Server and Logger and tells you which port
// number to use.
func testServerSetup(t *testing.T, ctx context.Context) (*Server, *logrus.Logger, int) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	// TODO replace real port with bufconn? This causes a firewall alert to trigger
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	require.Nil(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.Nil(t, err)
	port := func() int {
		defer l.Close() // TODO test context based close
		return l.Addr().(*net.TCPAddr).Port
	}()

	querierManager := NewQuerierManager(log)
	server := NewServer(fmt.Sprintf(":%d", port), 1024*1024*1024, false, querierManager, 100, log)
	enterrors.GoWrapper(func() {
		// TODO test context for shutdown instead of close
		err = server.Serve(ctx)
		require.Nil(t, err)
	}, log)
	return server, log, port
}

// testClientSetup gives you a ready to use Server and Logger and tells you which port
// number to use.
func testClientSetup(t *testing.T, port int, ctx context.Context) api.MetadataService_QuerierStreamClient {
	// TODO replace DialContex with NewClient
	//nolint:staticcheck
	leaderRpcConn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf(":%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.Nil(t, err)
	c := api.NewMetadataServiceClient(leaderRpcConn)
	client, err := c.QuerierStream(ctx)
	require.Nil(t, err)
	return client
}

// TestServerNotifyRecv tests that notifying the server.querierManager of a classTenantDataEvent
// propagates that to the client
func TestServerNotifyRecv(t *testing.T) {
	server, log, port := testServerSetup(t, context.Background())
	client := testClientSetup(t, port, context.Background())

	notifyCh := make(chan ClassTenant)
	enterrors.GoWrapper(func() {
		for ct := range notifyCh {
			err := server.querierManager.NotifyClassTenantDataEvent(ct)
			require.Nil(t, err)
		}
	}, log)

	defer client.CloseSend()
	classTenant := ClassTenant{"C1", "t1"}
	enterrors.GoWrapper(func() {
		<-time.After(10 * time.Millisecond)
		notifyCh <- classTenant
		close(notifyCh)
	}, log)
	resp, err := client.Recv()
	require.Nil(t, err)
	require.Equal(t, resp.ClassTenant.ClassName, classTenant.ClassName)
	require.Equal(t, resp.ClassTenant.TenantName, classTenant.TenantName)
}

// TestServerClientSendClose tests that the client closing the stream immediately works,
// even if the server tries to send the client an event
func TestServerClientSendClose(t *testing.T) {
	server, log, port := testServerSetup(t, context.Background())
	client := testClientSetup(t, port, context.Background())

	notifyCh := make(chan ClassTenant)
	enterrors.GoWrapper(func() {
		for ct := range notifyCh {
			err := server.querierManager.NotifyClassTenantDataEvent(ct)
			require.Nil(t, err)
		}
	}, log)

	err := client.CloseSend()
	require.Nil(t, err)
}

// TestServerClientDiesUnexpectedly tests that the server can handle the client dying
// unexpectedly (eg context cancelled)
func TestServerClientDiesUnexpectedly(t *testing.T) {
	server, log, port := testServerSetup(t, context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	client := testClientSetup(t, port, ctx)

	notifyCh := make(chan ClassTenant)
	enterrors.GoWrapper(func() {
		for ct := range notifyCh {
			err := server.querierManager.NotifyClassTenantDataEvent(ct)
			require.Nil(t, err)
		}
	}, log)
	time.AfterFunc(time.Millisecond, cancel)
	client.Recv()
}

// TestServerContextCancellation tests that the server closes if the serve context is cancelled
func TestServerContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server, log, port := testServerSetup(t, ctx)
	client := testClientSetup(t, port, context.Background())

	notifyCh := make(chan ClassTenant)
	enterrors.GoWrapper(func() {
		for ct := range notifyCh {
			err := server.querierManager.NotifyClassTenantDataEvent(ct)
			require.Nil(t, err)
		}
	}, log)
	time.AfterFunc(time.Millisecond, cancel)
	client.Recv()
}
