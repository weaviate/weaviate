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

package raftauth

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

var (
	rightCreds = cluster.BasicAuth{Username: "weaviate", Password: "secret"}
	wrongCreds = cluster.BasicAuth{Username: "weaviate", Password: "wrong"}
)

type peerKind string

const (
	kindLegacy      peerKind = "legacy"
	kindOff         peerKind = "off"
	kindSendRight   peerKind = "send-right"
	kindVerifyRight peerKind = "verify-right"
	kindSendWrong   peerKind = "send-wrong"
	kindVerifyWrong peerKind = "verify-wrong"
)

func (k peerKind) auth() cluster.BasicAuth {
	switch k {
	case kindSendRight:
		a := rightCreds
		a.SendOnly = true
		return a
	case kindVerifyRight:
		return rightCreds
	case kindSendWrong:
		a := wrongCreds
		a.SendOnly = true
		return a
	case kindVerifyWrong:
		return wrongCreds
	default:
		return cluster.BasicAuth{}
	}
}

type testPeer struct {
	trans  *raft.NetworkTransport
	stream *StreamLayer
	hook   *test.Hook
}

func newStreamLayer(t *testing.T, auth cluster.BasicAuth, timeout time.Duration) (*StreamLayer, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	sl, err := NewStreamLayer(Config{BindAddr: "127.0.0.1:0", Auth: auth, HandshakeTimeout: timeout, Logger: logger})
	require.NoError(t, err)
	return sl, hook
}

func newPeer(t *testing.T, kind peerKind, timeout time.Duration) *testPeer {
	t.Helper()
	if kind == kindLegacy {
		trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, timeout, io.Discard)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, trans.Close()) })
		return &testPeer{trans: trans}
	}
	sl, hook := newStreamLayer(t, kind.auth(), timeout)
	trans := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  sl,
		MaxPool: 2,
		Timeout: timeout,
		Logger:  hclog.NewNullLogger(),
	})
	t.Cleanup(func() { require.NoError(t, trans.Close()) })
	return &testPeer{trans: trans, stream: sl, hook: hook}
}

func respond(t *testing.T, trans *raft.NetworkTransport) <-chan any {
	t.Helper()
	logger, _ := test.NewNullLogger()
	delivered := make(chan any, 64)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	enterrors.GoWrapper(func() {
		for {
			select {
			case <-done:
				return
			case rpc := <-trans.Consumer():
				delivered <- rpc.Command
				rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
			}
		}
	}, logger)
	return delivered
}

func appendEntries(client *raft.NetworkTransport, target raft.ServerAddress) error {
	var resp raft.AppendEntriesResponse
	return client.AppendEntries("server", target, &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{Addr: []byte(client.LocalAddr())},
		Term:      1,
		Entries:   []*raft.Log{{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("x")}},
	}, &resp)
}

func hasWarn(hook *test.Hook, substr string) bool {
	if hook == nil {
		return false
	}
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}

func TestHandshakeMatrix(t *testing.T) {
	const mismatch = "peer credentials mismatch"
	tests := []struct {
		server, client peerKind
		wantOK         bool
		wantFallback   bool
		wantServerWarn bool
		wantClientWarn bool
	}{
		{server: kindLegacy, client: kindLegacy, wantOK: true},
		{server: kindLegacy, client: kindOff, wantOK: true},
		{server: kindLegacy, client: kindSendRight, wantOK: true, wantFallback: true},
		{server: kindLegacy, client: kindVerifyRight},
		{server: kindLegacy, client: kindSendWrong, wantOK: true, wantFallback: true},
		{server: kindLegacy, client: kindVerifyWrong},

		{server: kindOff, client: kindLegacy, wantOK: true},
		{server: kindOff, client: kindOff, wantOK: true},
		{server: kindOff, client: kindSendRight, wantOK: true},
		{server: kindOff, client: kindVerifyRight},
		{server: kindOff, client: kindSendWrong, wantOK: true},
		{server: kindOff, client: kindVerifyWrong},

		{server: kindSendRight, client: kindLegacy, wantOK: true},
		{server: kindSendRight, client: kindOff, wantOK: true},
		{server: kindSendRight, client: kindSendRight, wantOK: true},
		{server: kindSendRight, client: kindVerifyRight, wantOK: true},
		{server: kindSendRight, client: kindSendWrong, wantOK: true, wantServerWarn: true, wantClientWarn: true},
		{server: kindSendRight, client: kindVerifyWrong},

		{server: kindVerifyRight, client: kindLegacy},
		{server: kindVerifyRight, client: kindOff},
		{server: kindVerifyRight, client: kindSendRight, wantOK: true},
		{server: kindVerifyRight, client: kindVerifyRight, wantOK: true},
		{server: kindVerifyRight, client: kindSendWrong},
		{server: kindVerifyRight, client: kindVerifyWrong},
	}
	for _, tt := range tests {
		t.Run(string(tt.server)+"/"+string(tt.client), func(t *testing.T) {
			server := newPeer(t, tt.server, 2*time.Second)
			delivered := respond(t, server.trans)
			client := newPeer(t, tt.client, 2*time.Second)

			err := appendEntries(client.trans, server.trans.LocalAddr())
			if tt.wantOK {
				require.NoError(t, err)
				assert.Len(t, delivered, 1)
			} else {
				require.Error(t, err)
				assert.Empty(t, delivered)
			}
			if client.stream != nil {
				assert.Equal(t, tt.wantFallback, client.stream.isLegacy(string(server.trans.LocalAddr())))
			}
			assert.Equal(t, tt.wantServerWarn, hasWarn(server.hook, mismatch))
			assert.Equal(t, tt.wantClientWarn, hasWarn(client.hook, mismatch))
		})
	}
}

type impostor struct {
	addr     string
	accepted atomic.Int32
	first    chan byte
}

func newImpostor(t *testing.T, handle func(net.Conn)) *impostor {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	imp := &impostor{addr: l.Addr().String(), first: make(chan byte, 16)}
	logger, _ := test.NewNullLogger()
	enterrors.GoWrapper(func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			imp.accepted.Add(1)
			var b [1]byte
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err == nil {
				if _, err := io.ReadFull(conn, b[:]); err == nil {
					imp.first <- b[0]
					handle(conn)
				}
			}
			if err := conn.Close(); err != nil {
				logger.Debugf("close: %v", err)
			}
		}
	}, logger)
	return imp
}

func badProofServer(conn net.Conn) {
	var rest [helloLen - 1]byte
	if _, err := io.ReadFull(conn, rest[:]); err != nil {
		return
	}
	var sh [serverHelloLen]byte
	sh[0], sh[1], sh[2] = magic, version, flagCreds
	if _, err := rand.Read(sh[3:]); err != nil {
		return
	}
	if _, err := conn.Write(sh[:]); err != nil {
		return
	}
	var proof [proofLen]byte
	if _, err := io.ReadFull(conn, proof[:]); err != nil {
		return
	}
	if _, err := conn.Write([]byte{statusOK}); err != nil {
		return
	}
}

func noCredsServer(conn net.Conn) {
	var rest [helloLen - 1]byte
	if _, err := io.ReadFull(conn, rest[:]); err != nil {
		return
	}
	var sh [serverHelloLen]byte
	sh[0], sh[1] = magic, version
	if _, err := conn.Write(sh[:]); err != nil {
		return
	}
	var proof [proofLen]byte
	if _, err := io.ReadFull(conn, proof[:]); err != nil {
		return
	}
	if _, err := conn.Write([]byte{statusOK}); err != nil {
		return
	}
}

func TestVerifyingClientRefusesImpostorWithoutPlainRedial(t *testing.T) {
	tests := []struct {
		name   string
		handle func(net.Conn)
	}{
		{name: "closes after the first byte", handle: func(net.Conn) {}},
		{name: "sends a bad proof", handle: badProofServer},
		{name: "claims no credentials", handle: noCredsServer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imp := newImpostor(t, tt.handle)
			sl, _ := newStreamLayer(t, rightCreds, time.Second)
			t.Cleanup(func() { require.NoError(t, sl.Close()) })

			conn, err := sl.Dial(raft.ServerAddress(imp.addr), time.Second)
			require.Error(t, err)
			assert.Nil(t, conn)
			assert.False(t, sl.isLegacy(imp.addr))
			assert.Equal(t, magic, <-imp.first)
			assert.Never(t, func() bool { return imp.accepted.Load() > 1 }, 200*time.Millisecond, 10*time.Millisecond)
		})
	}
}

func TestSendOnlyClientLegacyFallbackAndReprobe(t *testing.T) {
	imp := newImpostor(t, func(net.Conn) {})
	auth := rightCreds
	auth.SendOnly = true
	sl, _ := newStreamLayer(t, auth, time.Second)
	t.Cleanup(func() { require.NoError(t, sl.Close()) })
	var offset atomic.Int64
	sl.now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }

	dialAndWrite := func() {
		conn, err := sl.Dial(raft.ServerAddress(imp.addr), time.Second)
		require.NoError(t, err)
		_, err = conn.Write([]byte{0})
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}

	dialAndWrite()
	assert.Equal(t, magic, <-imp.first)
	assert.Equal(t, byte(0), <-imp.first)
	assert.True(t, sl.isLegacy(imp.addr))

	dialAndWrite()
	assert.Equal(t, byte(0), <-imp.first)

	offset.Store(int64(LegacyTTL + time.Second))
	assert.False(t, sl.isLegacy(imp.addr))
	dialAndWrite()
	assert.Equal(t, magic, <-imp.first)
	assert.Equal(t, byte(0), <-imp.first)
	assert.True(t, sl.isLegacy(imp.addr))
}

func TestStalledHandshakeIsClosedAtDeadlineWithoutBlockingAccept(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "nothing sent", payload: nil},
		{name: "magic only", payload: []byte{magic}},
		{name: "hello without proof", payload: append([]byte{magic, 'W', 'R', 'A', version}, make([]byte, nonceLen)...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newPeer(t, kindVerifyRight, 300*time.Millisecond)
			delivered := respond(t, server.trans)

			stalled, err := net.Dial("tcp", string(server.trans.LocalAddr()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, stalled.Close()) })
			if len(tt.payload) > 0 {
				_, err = stalled.Write(tt.payload)
				require.NoError(t, err)
			}

			client := newPeer(t, kindVerifyRight, 2*time.Second)
			require.NoError(t, appendEntries(client.trans, server.trans.LocalAddr()))
			assert.Len(t, delivered, 1)

			require.NoError(t, stalled.SetReadDeadline(time.Now().Add(5*time.Second)))
			_, err = io.Copy(io.Discard, stalled)
			assert.False(t, errors.Is(err, os.ErrDeadlineExceeded), "server did not close the stalled connection: %v", err)
			assert.True(t, hasWarn(server.hook, "raft auth"))
		})
	}
}

func rawClientHello(t *testing.T, conn net.Conn, clientNonce []byte) (serverNonce []byte) {
	t.Helper()
	hello := append([]byte{magic, 'W', 'R', 'A', version}, clientNonce...)
	_, err := conn.Write(hello)
	require.NoError(t, err)
	var sh [serverHelloLen]byte
	_, err = io.ReadFull(conn, sh[:])
	require.NoError(t, err)
	require.Equal(t, flagCreds, sh[2])
	return append([]byte(nil), sh[3:3+nonceLen]...)
}

func rawClientProof(key, serverNonce, clientNonce []byte) []byte {
	var out [proofLen]byte
	return computeProof(hmac.New(sha256.New, key), 'C', serverNonce, clientNonce, out[:])
}

func readStatus(t *testing.T, conn net.Conn) byte {
	t.Helper()
	var status [1]byte
	_, err := io.ReadFull(conn, status[:])
	require.NoError(t, err)
	return status[0]
}

func TestHandshakeAndPayloadInOneWriteLoseNoBytes(t *testing.T) {
	sl, _ := newStreamLayer(t, rightCreds, 2*time.Second)
	t.Cleanup(func() { require.NoError(t, sl.Close()) })
	payload := make([]byte, 3*serverReadBufferSize+17)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	received := make(chan []byte, 1)
	logger, _ := test.NewNullLogger()
	enterrors.GoWrapper(func() {
		conn, err := sl.Accept()
		if err != nil {
			received <- nil
			return
		}
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, buf); err != nil {
			buf = nil
		}
		received <- buf
		if err := conn.Close(); err != nil {
			logger.Debugf("close: %v", err)
		}
	}, logger)

	conn, err := net.Dial("tcp", sl.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	clientNonce := make([]byte, nonceLen)
	_, err = rand.Read(clientNonce)
	require.NoError(t, err)
	serverNonce := rawClientHello(t, conn, clientNonce)

	_, err = conn.Write(append(rawClientProof(sl.key, serverNonce, clientNonce), payload...))
	require.NoError(t, err)
	assert.Equal(t, statusOK, readStatus(t, conn))
	select {
	case got := <-received:
		assert.True(t, bytes.Equal(payload, got))
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the payload sent with the proof")
	}
}

func TestReplayedProofIsRejected(t *testing.T) {
	server := newPeer(t, kindVerifyRight, 2*time.Second)
	delivered := respond(t, server.trans)
	key := deriveKey(rightCreds)
	clientNonce := make([]byte, nonceLen)
	_, err := rand.Read(clientNonce)
	require.NoError(t, err)

	first, err := net.Dial("tcp", string(server.trans.LocalAddr()))
	require.NoError(t, err)
	captured := rawClientProof(key, rawClientHello(t, first, clientNonce), clientNonce)
	_, err = first.Write(captured)
	require.NoError(t, err)
	require.Equal(t, statusOK, readStatus(t, first))
	require.NoError(t, first.Close())

	replay, err := net.Dial("tcp", string(server.trans.LocalAddr()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replay.Close()) })
	rawClientHello(t, replay, clientNonce)
	_, err = replay.Write(captured)
	require.NoError(t, err)
	assert.Equal(t, statusRejected, readStatus(t, replay))
	assert.Empty(t, delivered)
}

func TestNewStreamLayerRejectsUnadvertisableAddress(t *testing.T) {
	logger, _ := test.NewNullLogger()
	_, err := NewStreamLayer(Config{BindAddr: "0.0.0.0:0", Logger: logger})
	require.ErrorIs(t, err, errNotAdvertisable)

	sl, err := NewStreamLayer(Config{
		BindAddr:  "0.0.0.0:0",
		Advertise: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8300},
		Logger:    logger,
	})
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8300", sl.Addr().String())
	require.NoError(t, sl.Close())

	_, err = NewStreamLayer(Config{BindAddr: "127.0.0.1:0", Advertise: &net.UDPAddr{IP: net.ParseIP("127.0.0.1")}, Logger: logger})
	require.ErrorIs(t, err, errNotTCP)
}
