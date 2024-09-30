package metadataserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/exp/metadataserver/proto/api"
	"google.golang.org/grpc/metadata"
)

func TestServer(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.Nil(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.Nil(t, err)
	port := func() int {
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}()

	querierManager := NewQuerierManager()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	server := NewServer(fmt.Sprintf(":%d", port), 1024*1024*1024, false, querierManager, log)
	err = server.Open()
	require.Nil(t, err)

	// TODO test with actual grpc client
}

// TODO go way to do this?
func testQuerierStreamSetup() (*MockMetadataServiceQuerierStreamServer, *Server, *QuerierManager, *logrus.Logger) {
	querierManager := NewQuerierManager()
	log := logrus.New()
	server := NewServer(
		"",
		0,
		false,
		querierManager,
		log,
	)
	stream := &MockMetadataServiceQuerierStreamServer{}
	return stream, server, querierManager, log
}

func TestQuerierStreamClosedImmediately(t *testing.T) {
	stream, server, _, _ := testQuerierStreamSetup()
	stream.On("Recv").Return(&api.QuerierStreamRequest{}, io.EOF)
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	stream.On("Context").Return(cancelledContext)
	err := server.QuerierStream(stream)
	require.Nil(t, err)
}

func TestQuerierStreamClosedImmediatelyDuringNotify(t *testing.T) {
	stream, server, querierManager, log := testQuerierStreamSetup()
	stream.On("Recv").Return(&api.QuerierStreamRequest{}, io.EOF)
	stream.On("Send", mock.Anything).Return(nil)
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	stream.On("Context").Return(cancelledContext)
	enterrors.GoWrapper(func() {
		waitUntilNQueriersRegistered(querierManager, 1)
		querierManager.NotifyClassTenantDataEvent(ClassTenant{
			ClassName:  "C1",
			TenantName: "t1",
		})
	}, log)
	err := server.QuerierStream(stream)
	require.Nil(t, err)
}

func TestQuerierStreamNotifySends(t *testing.T) {
	stream, server, querierManager, log := testQuerierStreamSetup()

	sendCh := make(chan time.Time)
	contextCh := make(chan time.Time)

	className := "C1"
	tenantName := "t1"
	stream.On("Recv").Return(&api.QuerierStreamRequest{}, io.EOF)
	sendArg := &api.QuerierStreamResponse{
		Type: api.QuerierStreamResponse_TYPE_CLASS_TENANT_DATA_UPDATE,
		ClassTenant: &api.ClassTenant{
			ClassName:  className,
			TenantName: tenantName,
		},
	}
	sendCall := stream.On("Send", sendArg).Return(nil).WaitUntil(sendCh)
	stream.On("Context").Return(context.Background()).Once()
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	stream.On("Context").Return(cancelledContext).WaitUntil(contextCh)

	enterrors.GoWrapper(func() {
		waitUntilNQueriersRegistered(querierManager, 1)
		querierManager.NotifyClassTenantDataEvent(ClassTenant{
			ClassName:  className,
			TenantName: tenantName,
		})
		sendCh <- time.Time{}
		contextCh <- time.Time{}
	}, log)
	err := server.QuerierStream(stream)
	require.Nil(t, err)
	sendCall.Parent.AssertNumberOfCalls(t, "Send", 1)
}

type MockMetadataServiceQuerierStreamServer struct {
	mock.Mock
}

func (m *MockMetadataServiceQuerierStreamServer) Send(resp *api.QuerierStreamResponse) error {
	args := m.Called(resp)
	return args.Error(0)
}

func (m *MockMetadataServiceQuerierStreamServer) Recv() (*api.QuerierStreamRequest, error) {
	args := m.Called()
	return args.Get(0).(*api.QuerierStreamRequest), args.Error(1)
}

func (m *MockMetadataServiceQuerierStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (m *MockMetadataServiceQuerierStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (m *MockMetadataServiceQuerierStreamServer) SetTrailer(metadata.MD) {}

func (m *MockMetadataServiceQuerierStreamServer) Context() context.Context {
	args := m.Called()
	return args.Get(0).(context.Context)
}

func (m *MockMetadataServiceQuerierStreamServer) SendMsg(msg any) error {
	return nil
}

func (m *MockMetadataServiceQuerierStreamServer) RecvMsg(msg any) error {
	return nil
}

func waitUntilNQueriersRegistered(qm *QuerierManager, n int) {
	for {
		numQueriersRegistered := 0
		qm.registeredQueriers.Range(func(_, _ any) bool {
			numQueriersRegistered++
			return false
		})
		if numQueriersRegistered > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
}
