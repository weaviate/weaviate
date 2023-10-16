//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"context"
	"log"
	"strings"
	"time"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	localService Cluster
}

func NewClient(localService Cluster) *Client {
	return &Client{localService: localService}
}

func (cl Client) Join(leaderAddr string, req *cmd.JoinPeerRequest) error {
	log.Printf("client join: %s %+v\n", leaderAddr, req)
	if leaderAddr == cl.localService.address {
		err := cl.localService.Join(req.Id, req.Address, req.Voter)
		if err == nil {
			return err
		}
	}

	conn, err := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.JoinPeer(ctx, req)
	if err != nil {
		return err
	}
	log.Printf("join response: %s", r.String())
	return nil
}

func (cl Client) Remove(leaderAddress string, req *cmd.RemovePeerRequest) error {
	log.Printf("client remove: %s %+v\n", leaderAddress, req)
	if cl.localService.address == leaderAddress {
		return cl.localService.Remove(req.Id)
	}
	// lAddr := cl.leaderAddress()
	// if lAddr == "" {
	// 	return err
	// }
	conn, err := grpc.Dial(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := cmd.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.RemovePeer(ctx, req)
	if err != nil {
		return err
	}
	log.Printf("join response: %s", r.String())
	return nil
}

// TODO-RAFT
// leaderAddress is just a workaround to get address of leader rpc  service
func (cl Client) leaderAddress() string {
	lAddr, _ := cl.localService.Raft.LeaderWithID()
	if lAddr == "" {
		return ""
	}
	xs := strings.Split(string(lAddr), ":")
	return xs[0] + ":2" + xs[1][1:]
}
