package clients

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type RemoteIndex struct {
	client *http.Client
}

func NewRemoteIndex(httpClient *http.Client) *RemoteIndex {
	return &RemoteIndex{client: httpClient}
}

func (c *RemoteIndex) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object) error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := obj.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshal payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	req.Header.Set("content-type", "application/vnd.weaviate.storobj+octet-stream")

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}
	return out
}

func marshalObjsList(in []*storobj.Object) ([]byte, error) {
	// NOTE: This implementation is not optimized for allocation efficiency,
	// reserve 1024 byte per object which is rather arbitrary
	out := make([]byte, 0, 1024*len(in))

	reusableLengthBuf := make([]byte, 8)
	for _, ind := range in {
		bytes, err := ind.MarshalBinary()
		if err != nil {
			return nil, err
		}

		length := uint64(len(bytes))
		binary.LittleEndian.PutUint64(reusableLengthBuf, length)

		out = append(out, reusableLengthBuf...)
		out = append(out, bytes...)
	}

	return out, nil
}

func (c *RemoteIndex) BatchPutObjects(ctx context.Context, hostName, indexName,
	shardName string, objs []*storobj.Object) []error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := marshalObjsList(objs)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "marshal payload"), len(objs))
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return duplicateErr(errors.Wrap(err, "open http request"), len(objs))
	}

	req.Header.Set("content-type", "application/vnd.weaviate.storobj.list+octet-stream")

	res, err := c.client.Do(req)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "send http request"), len(objs))
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return duplicateErr(errors.Errorf("unexpected status code %d (%s)",
			res.StatusCode, body), len(objs))
	}

	ct := res.Header.Get("content-type")
	if ct != "application/vnd.weaviate.error.list+json" {
		return duplicateErr(errors.Errorf("unexpected content type: %s",
			ct), len(objs))
	}

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "ready body"), len(objs))
	}

	return unmarshalErrList(resBytes)
}

func unmarshalErrList(in []byte) []error {
	var msgs []interface{}
	json.Unmarshal(in, &msgs)

	converted := make([]error, len(msgs))

	for i, msg := range msgs {
		if msg == nil {
			continue
		}

		converted[i] = errors.New(msg.(string))
	}

	return converted
}

func (c *RemoteIndex) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, selectProps search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	selectPropsBytes, err := json.Marshal(selectProps)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}

	additionalBytes, err := json.Marshal(additional)
	if err != nil {
		return nil, errors.Wrap(err, "marshal additional props")
	}

	selectPropsEncoded := base64.StdEncoding.EncodeToString(selectPropsBytes)
	additionalEncoded := base64.StdEncoding.EncodeToString(additionalBytes)

	path := fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	q := url.Query()
	q.Set("additional", additionalEncoded)
	q.Set("selectProperties", selectPropsEncoded)
	url.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		// this is a legitimate case - the requested ID doesn't exist, don't try
		// to unmarshal anything
		return nil, nil
	}

	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	objBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	obj, err := storobj.FromBinary(objBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal body")
	}

	return obj, nil
}

func (c *RemoteIndex) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID) ([]*storobj.Object, error) {
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}

	idsEncoded := base64.StdEncoding.EncodeToString(idsBytes)

	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	q := url.Query()
	q.Set("ids", idsEncoded)
	url.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		// this is a legitimate case - the requested ID doesn't exist, don't try
		// to unmarshal anything
		return nil, nil
	}

	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	ct := res.Header.Get("content-type")
	if ct != "application/vnd.weaviate.storobj.list+octet-stream" {
		return nil, errors.Errorf("unexpected content type: %s", ct)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	objs, err := unmarshalObjsList(bodyBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal objects")
	}

	return objs, nil
}

type searchParametersPayload struct {
	SearchVector []float32             `json:"searchVector"`
	Limit        int                   `json:"limit"`
	Filters      *filters.LocalFilter  `json:"filters"`
	Additional   additional.Properties `json:"additional"`
}

func (c *RemoteIndex) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []float32, limit int, filters *filters.LocalFilter,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	params := searchParametersPayload{
		SearchVector: vector,
		Limit:        limit,
		Filters:      filters,
		Additional:   additional,
	}

	paramsBytes, err := json.Marshal(params)
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshal request payload")
	}

	path := fmt.Sprintf("/indices/%s/shards/%s/objects/_search", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(paramsBytes))
	if err != nil {
		return nil, nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(res.Body)
		return nil, nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, errors.Wrap(err, "read body")
	}

	objs, dists, err := unmarshalSearchResultsPayload(resBytes)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unmarshal body")
	}
	return objs, dists, nil
}

func unmarshalObjsList(in []byte) ([]*storobj.Object, error) {
	// NOTE: This implementation is not optimized for allocation efficiency
	var out []*storobj.Object

	reusableLengthBuf := make([]byte, 8)
	r := bytes.NewReader(in)

	for {
		_, err := r.Read(reusableLengthBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		payloadBytes := make([]byte, binary.LittleEndian.Uint64(reusableLengthBuf))
		_, err = r.Read(payloadBytes)
		if err != nil {
			return nil, err
		}

		obj, err := storobj.FromBinary(payloadBytes)
		if err != nil {
			return nil, err
		}

		out = append(out, obj)
	}

	return out, nil
}

func unmarshalSearchResultsPayload(in []byte) ([]*storobj.Object,
	[]float32, error) {
	read := uint64(0)

	objsLength := binary.LittleEndian.Uint64(in[read : read+8])
	read += 8

	objs, err := unmarshalObjsList(in[read : read+objsLength])
	if err != nil {
		return nil, nil, err
	}
	read += objsLength

	distsLength := binary.LittleEndian.Uint64(in[read : read+8])
	read += 8

	dists := make([]float32, distsLength)
	for i := range dists {
		dists[i] = math.Float32frombits(binary.LittleEndian.Uint32(in[read : read+4]))
		read += 4
	}

	if read != uint64(len(in)) {
		return nil, nil, errors.Errorf("corrupt read: %d != %d", read, len(in))
	}

	return objs, dists, nil
}
