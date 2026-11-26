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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"syscall"

	"github.com/weaviate/weaviate/usecases/cluster"
)

// Handshake v1, client speaks first, every frame has a fixed size:
//
//	C->S hello:       magic | "WRA" | version | clientNonce[32]
//	S->C serverHello: magic | version | flags | serverNonce[32] | serverProof[32]
//	C->S clientProof: HMAC(K, "C" | serverNonce | clientNonce)
//	S->C status:      statusOK or statusRejected
//
// serverProof is HMAC(K, "S" | clientNonce | serverNonce), or zeros when the
// server has no credentials (flagCreds unset).
const (
	// magic is never a raft rpc type (0..4), so a legacy server rejects it and a new server can tell both apart.
	magic   byte = 0xC5
	version byte = 1

	nonceLen = 32
	proofLen = sha256.Size

	helloLen       = 1 + 3 + 1 + nonceLen
	serverHelloLen = 1 + 1 + 1 + nonceLen + proofLen

	flagCreds byte = 1

	statusOK       byte = 0
	statusRejected byte = 1

	keyContext = "weaviate-raft-auth-v1"
)

var helloTag = [3]byte{'W', 'R', 'A'}

var (
	errLegacyPeer      = errors.New("raft auth: peer closed the connection before answering the handshake")
	errRejected        = errors.New("raft auth: peer rejected our credentials")
	errBadFrame        = errors.New("raft auth: malformed handshake frame")
	errPeerNoCreds     = errors.New("raft auth: peer has no credentials configured")
	errProofMismatch   = errors.New("raft auth: peer credentials mismatch")
	errUnauthenticated = errors.New("raft auth: peer did not authenticate")
)

// deriveKey returns the HMAC key, or nil when no credentials are configured.
func deriveKey(ba cluster.BasicAuth) []byte {
	if !ba.Enabled() {
		return nil
	}
	buf := make([]byte, 0, len(keyContext)+8+len(ba.Username)+len(ba.Password))
	buf = append(buf, keyContext...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ba.Username)))
	buf = append(buf, ba.Username...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ba.Password)))
	buf = append(buf, ba.Password...)
	sum := sha256.Sum256(buf)
	return sum[:]
}

func computeProof(mac hash.Hash, label byte, first, second []byte, out []byte) []byte {
	mac.Reset()
	mac.Write([]byte{label})
	mac.Write(first)
	mac.Write(second)
	return mac.Sum(out[:0])
}

// isLegacyClose reports whether a read error looks like a legacy server that dropped the unknown rpc type.
func isLegacyClose(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)
}

// clientHandshake runs the client side on rw. The caller sets the deadline.
func clientHandshake(rw io.ReadWriter, key []byte, verify bool) (mismatch bool, err error) {
	var hello [helloLen]byte
	hello[0] = magic
	copy(hello[1:4], helloTag[:])
	hello[4] = version
	clientNonce := hello[5:]
	if _, err := rand.Read(clientNonce); err != nil {
		return false, fmt.Errorf("raft auth: client nonce: %w", err)
	}
	if _, err := rw.Write(hello[:]); err != nil {
		return false, fmt.Errorf("raft auth: send hello: %w", err)
	}

	var sh [serverHelloLen]byte
	if _, err := io.ReadFull(rw, sh[:]); err != nil {
		if isLegacyClose(err) {
			return false, fmt.Errorf("%w: %w", errLegacyPeer, err)
		}
		return false, fmt.Errorf("raft auth: read server hello: %w", err)
	}
	if sh[0] != magic || sh[1] != version {
		return false, errBadFrame
	}
	serverCreds := sh[2]&flagCreds != 0
	serverNonce := sh[3 : 3+nonceLen]
	serverProof := sh[3+nonceLen:]

	mac := hmac.New(sha256.New, key)
	var expected [proofLen]byte
	proofOK := serverCreds && hmac.Equal(serverProof, computeProof(mac, 'S', clientNonce, serverNonce, expected[:]))
	if verify {
		if !serverCreds {
			return false, errPeerNoCreds
		}
		if !proofOK {
			return false, errProofMismatch
		}
	}

	var proof [proofLen]byte
	if _, err := rw.Write(computeProof(mac, 'C', serverNonce, clientNonce, proof[:])); err != nil {
		return false, fmt.Errorf("raft auth: send proof: %w", err)
	}

	var status [1]byte
	if _, err := io.ReadFull(rw, status[:]); err != nil {
		return false, fmt.Errorf("raft auth: read status: %w", err)
	}
	if status[0] != statusOK {
		return false, errRejected
	}
	return serverCreds && !proofOK, nil
}

// serverHandshake runs the server side on r and w after the magic byte was
// peeked. With verify set a missing or wrong client proof is an error;
// otherwise it is reported as mismatch.
func serverHandshake(r io.Reader, w io.Writer, key []byte, verify bool) (mismatch bool, err error) {
	var hello [helloLen]byte
	if _, err := io.ReadFull(r, hello[:]); err != nil {
		return false, fmt.Errorf("raft auth: read hello: %w", err)
	}
	if hello[0] != magic || [3]byte(hello[1:4]) != helloTag || hello[4] != version {
		return false, errBadFrame
	}
	clientNonce := hello[5:]

	var sh [serverHelloLen]byte
	sh[0] = magic
	sh[1] = version
	serverNonce := sh[3 : 3+nonceLen]
	if _, err := rand.Read(serverNonce); err != nil {
		return false, fmt.Errorf("raft auth: server nonce: %w", err)
	}
	var mac hash.Hash
	if key != nil {
		sh[2] = flagCreds
		mac = hmac.New(sha256.New, key)
		var serverProof [proofLen]byte
		copy(sh[3+nonceLen:], computeProof(mac, 'S', clientNonce, serverNonce, serverProof[:]))
	}
	if _, err := w.Write(sh[:]); err != nil {
		return false, fmt.Errorf("raft auth: send server hello: %w", err)
	}

	var proof [proofLen]byte
	if _, err := io.ReadFull(r, proof[:]); err != nil {
		return false, fmt.Errorf("raft auth: read proof: %w", err)
	}
	proofOK := false
	if mac != nil {
		var expected [proofLen]byte
		proofOK = hmac.Equal(proof[:], computeProof(mac, 'C', serverNonce, clientNonce, expected[:]))
	}

	if verify && !proofOK {
		if _, err := w.Write([]byte{statusRejected}); err != nil {
			return false, fmt.Errorf("%w (send reject: %w)", errUnauthenticated, err)
		}
		return false, errUnauthenticated
	}
	if _, err := w.Write([]byte{statusOK}); err != nil {
		return false, fmt.Errorf("raft auth: send status: %w", err)
	}
	return mac != nil && !proofOK, nil
}
