package clients

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type RemoteIndex struct {
	client *http.Client
}

func NewRemoteIndex(httpClient *http.Client) *RemoteIndex {
	return &RemoteIndex{client: httpClient}
}

func (c *RemoteIndex) PutObject(ctx context.Context, hostName, shardName string,
	obj *storobj.Object) error {
	return errors.Errorf("not implemented")
}
