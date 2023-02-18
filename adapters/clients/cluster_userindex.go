package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/userindex"
)

type UserIndex struct {
	client *http.Client
}

func NewUserIndex(client *http.Client) *UserIndex {
	return &UserIndex{client: client}
}

func (c *UserIndex) UserIndexStatus(ctx context.Context,
	hostName string, className string,
) ([]userindex.Index, error) {
	path := "/userindexes/" + className

	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return nil, enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	var out []userindex.Index
	err = json.Unmarshal(body, &out)
	if err != nil {
		return nil, enterrors.NewErrUnmarshalBody(err)
	}

	return out, nil
}
