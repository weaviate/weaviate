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

package fakes

type FakeRPCAddressResolver struct {
	addr string
	err  error
}

func NewFakeRPCAddressResolver(addr string, err error) *FakeRPCAddressResolver {
	return &FakeRPCAddressResolver{addr: addr, err: err}
}

func (m *FakeRPCAddressResolver) Address(raftAddress string) (string, error) {
	return m.addr, m.err
}
