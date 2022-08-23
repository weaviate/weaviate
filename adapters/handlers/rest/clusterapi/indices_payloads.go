//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clusterapi

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

var IndicesPayloads = indicesPayloads{}

type indicesPayloads struct {
	ErrorList                 errorListPayload
	SingleObject              singleObjectPayload
	MergeDoc                  mergeDocPayload
	ObjectList                objectListPayload
	SearchResults             searchResultsPayload
	SearchParams              searchParamsPayload
	ReferenceList             referenceListPayload
	AggregationParams         aggregationParamsPayload
	AggregationResult         aggregationResultPayload
	FindDocIDsParams          findDocIDsParamsPayload
	FindDocIDsResults         findDocIDsResultsPayload
	BatchDeleteParams         batchDeleteParamsPayload
	BatchDeleteResults        batchDeleteResultsPayload
	GetShardStatusParams      getShardStatusParamsPayload
	GetShardStatusResults     getShardStatusResultsPayload
	UpdateShardStatusParams   updateShardStatusParamsPayload
	UpdateShardsStatusResults updateShardsStatusResultsPayload
}

type errorListPayload struct{}

func (e errorListPayload) MIME() string {
	return "application/vnd.weaviate.error.list+json"
}

func (e errorListPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", e.MIME())
}

func (e errorListPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == e.MIME()
}

func (e errorListPayload) Marshal(in []error) ([]byte, error) {
	converted := make([]interface{}, len(in))
	for i, err := range in {
		if err == nil {
			continue
		}

		converted[i] = err.Error()
	}

	return json.Marshal(converted)
}

func (e errorListPayload) Unmarshal(in []byte) []error {
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

type singleObjectPayload struct{}

func (p singleObjectPayload) MIME() string {
	return "application/vnd.weaviate.storobj+octet-stream"
}

func (p singleObjectPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p singleObjectPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p singleObjectPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p singleObjectPayload) Marshal(in *storobj.Object) ([]byte, error) {
	return in.MarshalBinary()
}

func (p singleObjectPayload) Unmarshal(in []byte) (*storobj.Object, error) {
	return storobj.FromBinary(in)
}

type objectListPayload struct{}

func (p objectListPayload) MIME() string {
	return "application/vnd.weaviate.storobj.list+octet-stream"
}

func (p objectListPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p objectListPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p objectListPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p objectListPayload) Marshal(in []*storobj.Object) ([]byte, error) {
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

func (p objectListPayload) Unmarshal(in []byte) ([]*storobj.Object, error) {
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

type mergeDocPayload struct{}

func (p mergeDocPayload) MIME() string {
	return "application/vnd.weaviate.mergedoc+json"
}

func (p mergeDocPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p mergeDocPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p mergeDocPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p mergeDocPayload) Marshal(in objects.MergeDocument) ([]byte, error) {
	// assumes that this type is fully json-marshable. Not the most
	// bandwidth-efficient way, but this is unlikely to become a bottleneck. If it
	// does, a custom binary marshaller might be more appropriate
	return json.Marshal(in)
}

func (p mergeDocPayload) Unmarshal(in []byte) (objects.MergeDocument, error) {
	var mergeDoc objects.MergeDocument
	err := json.Unmarshal(in, &mergeDoc)
	return mergeDoc, err
}

type searchParamsPayload struct{}

func (p searchParamsPayload) Marshal(vector []float32, limit int,
	filter *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort, addP additional.Properties,
) ([]byte, error) {
	type params struct {
		SearchVector   []float32                    `json:"searchVector"`
		Limit          int                          `json:"limit"`
		Filters        *filters.LocalFilter         `json:"filters"`
		KeywordRanking *searchparams.KeywordRanking `json:"keywordRanking"`
		Sort           []filters.Sort               `json:"sort"`
		Additional     additional.Properties        `json:"additional"`
	}

	par := params{vector, limit, filter, keywordRanking, sort, addP}
	return json.Marshal(par)
}

func (p searchParamsPayload) Unmarshal(in []byte) ([]float32, float32, int,
	*filters.LocalFilter, *searchparams.KeywordRanking, []filters.Sort, additional.Properties, error,
) {
	type searchParametersPayload struct {
		SearchVector   []float32                    `json:"searchVector"`
		Distance       float32                      `json:"distance"`
		Limit          int                          `json:"limit"`
		Filters        *filters.LocalFilter         `json:"filters"`
		KeywordRanking *searchparams.KeywordRanking `json:"keywordRanking"`
		Sort           []filters.Sort               `json:"sort"`
		Additional     additional.Properties        `json:"additional"`
	}
	var par searchParametersPayload
	err := json.Unmarshal(in, &par)
	return par.SearchVector, par.Distance, par.Limit,
		par.Filters, par.KeywordRanking, par.Sort, par.Additional, err
}

func (p searchParamsPayload) MIME() string {
	return "vnd.weaviate.searchparams+json"
}

func (p searchParamsPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p searchParamsPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

type searchResultsPayload struct{}

func (p searchResultsPayload) Unmarshal(in []byte) ([]*storobj.Object, []float32, error) {
	read := uint64(0)

	objsLength := binary.LittleEndian.Uint64(in[read : read+8])
	read += 8

	objs, err := IndicesPayloads.ObjectList.Unmarshal(in[read : read+objsLength])
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

func (p searchResultsPayload) Marshal(objs []*storobj.Object,
	dists []float32,
) ([]byte, error) {
	reusableLengthBuf := make([]byte, 8)
	var out []byte
	objsBytes, err := IndicesPayloads.ObjectList.Marshal(objs)
	if err != nil {
		return nil, err
	}

	objsLength := uint64(len(objsBytes))
	binary.LittleEndian.PutUint64(reusableLengthBuf, objsLength)

	out = append(out, reusableLengthBuf...)
	out = append(out, objsBytes...)

	distsLength := uint64(len(dists))
	binary.LittleEndian.PutUint64(reusableLengthBuf, distsLength)
	out = append(out, reusableLengthBuf...)

	distsBuf := make([]byte, distsLength*4)
	for i, dist := range dists {
		distUint32 := math.Float32bits(dist)
		binary.LittleEndian.PutUint32(distsBuf[(i*4):((i+1)*4)], distUint32)
	}
	out = append(out, distsBuf...)

	return out, nil
}

func (p searchResultsPayload) MIME() string {
	return "application/vnd.weaviate.shardsearchresults+octet-stream"
}

func (p searchResultsPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p searchResultsPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

type referenceListPayload struct{}

func (p referenceListPayload) MIME() string {
	return "application/vnd.weaviate.references.list+json"
}

func (p referenceListPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p referenceListPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p referenceListPayload) Marshal(in objects.BatchReferences) ([]byte, error) {
	// assumes that this type is fully json-marshable. Not the most
	// bandwidth-efficient way, but this is unlikely to become a bottleneck. If it
	// does, a custom binary marshaller might be more appropriate
	return json.Marshal(in)
}

func (p referenceListPayload) Unmarshal(in []byte) (objects.BatchReferences, error) {
	var out objects.BatchReferences
	err := json.Unmarshal(in, &out)
	return out, err
}

type aggregationParamsPayload struct{}

func (p aggregationParamsPayload) Marshal(params aggregation.Params) ([]byte, error) {
	// assumes that this type is fully json-marshable. Not the most
	// bandwidth-efficient way, but this is unlikely to become a bottleneck. If it
	// does, a custom binary marshaller might be more appropriate
	return json.Marshal(params)
}

func (p aggregationParamsPayload) Unmarshal(in []byte) (aggregation.Params, error) {
	var out aggregation.Params
	err := json.Unmarshal(in, &out)
	return out, err
}

func (p aggregationParamsPayload) MIME() string {
	return "application/vnd.weaviate.aggregations.params+json"
}

func (p aggregationParamsPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

func (p aggregationParamsPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

type aggregationResultPayload struct{}

func (p aggregationResultPayload) MIME() string {
	return "application/vnd.weaviate.aggregations.result+json"
}

func (p aggregationResultPayload) CheckContentTypeHeader(res *http.Response) (string, bool) {
	ct := res.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p aggregationResultPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p aggregationResultPayload) Marshal(in *aggregation.Result) ([]byte, error) {
	// assumes that this type is fully json-marshable. Not the most
	// bandwidth-efficient way, but this is unlikely to become a bottleneck. If it
	// does, a custom binary marshaller might be more appropriate
	return json.Marshal(in)
}

func (p aggregationResultPayload) Unmarshal(in []byte) (*aggregation.Result, error) {
	var out aggregation.Result
	err := json.Unmarshal(in, &out)
	return &out, err
}

type findDocIDsParamsPayload struct{}

func (p findDocIDsParamsPayload) Marshal(filter *filters.LocalFilter) ([]byte, error) {
	type params struct {
		Filters *filters.LocalFilter `json:"filters"`
	}

	par := params{filter}
	return json.Marshal(par)
}

func (p findDocIDsParamsPayload) Unmarshal(in []byte) (*filters.LocalFilter, error) {
	type findDocIDsParametersPayload struct {
		Filters *filters.LocalFilter `json:"filters"`
	}
	var par findDocIDsParametersPayload
	err := json.Unmarshal(in, &par)
	return par.Filters, err
}

func (p findDocIDsParamsPayload) MIME() string {
	return "vnd.weaviate.finddocidsparams+json"
}

func (p findDocIDsParamsPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p findDocIDsParamsPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

type findDocIDsResultsPayload struct{}

func (p findDocIDsResultsPayload) Unmarshal(in []byte) ([]uint64, error) {
	var out []uint64
	err := json.Unmarshal(in, &out)
	return out, err
}

func (p findDocIDsResultsPayload) Marshal(in []uint64) ([]byte, error) {
	return json.Marshal(in)
}

func (p findDocIDsResultsPayload) MIME() string {
	return "application/vnd.weaviate.finddocidsresults+octet-stream"
}

func (p findDocIDsResultsPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p findDocIDsResultsPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

type batchDeleteParamsPayload struct{}

func (p batchDeleteParamsPayload) Marshal(docIDs []uint64, dryRun bool) ([]byte, error) {
	type params struct {
		DocIDs []uint64 `json:"docIDs"`
		DryRun bool     `json:"dryRun"`
	}

	par := params{docIDs, dryRun}
	return json.Marshal(par)
}

func (p batchDeleteParamsPayload) Unmarshal(in []byte) ([]uint64, bool, error) {
	type batchDeleteParametersPayload struct {
		DocIDs []uint64 `json:"docIDs"`
		DryRun bool     `json:"dryRun"`
	}
	var par batchDeleteParametersPayload
	err := json.Unmarshal(in, &par)
	return par.DocIDs, par.DryRun, err
}

func (p batchDeleteParamsPayload) MIME() string {
	return "vnd.weaviate.batchdeleteparams+json"
}

func (p batchDeleteParamsPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p batchDeleteParamsPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

type batchDeleteResultsPayload struct{}

func (p batchDeleteResultsPayload) Unmarshal(in []byte) (objects.BatchSimpleObjects, error) {
	var out objects.BatchSimpleObjects
	err := json.Unmarshal(in, &out)
	return out, err
}

func (p batchDeleteResultsPayload) Marshal(in objects.BatchSimpleObjects) ([]byte, error) {
	return json.Marshal(in)
}

func (p batchDeleteResultsPayload) MIME() string {
	return "application/vnd.weaviate.batchdeleteresults+octet-stream"
}

func (p batchDeleteResultsPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p batchDeleteResultsPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

type getShardStatusParamsPayload struct{}

func (p getShardStatusParamsPayload) MIME() string {
	return "vnd.weaviate.getshardstatusparams+json"
}

func (p getShardStatusParamsPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p getShardStatusParamsPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

type getShardStatusResultsPayload struct{}

func (p getShardStatusResultsPayload) Unmarshal(in []byte) (string, error) {
	var out string
	err := json.Unmarshal(in, &out)
	return out, err
}

func (p getShardStatusResultsPayload) Marshal(in string) ([]byte, error) {
	return json.Marshal(in)
}

func (p getShardStatusResultsPayload) MIME() string {
	return "application/vnd.weaviate.getshardstatusresults+octet-stream"
}

func (p getShardStatusResultsPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p getShardStatusResultsPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

type updateShardStatusParamsPayload struct{}

func (p updateShardStatusParamsPayload) Marshal(targetStatus string) ([]byte, error) {
	type params struct {
		TargetStatus string `json:"targetStatus"`
	}

	par := params{targetStatus}
	return json.Marshal(par)
}

func (p updateShardStatusParamsPayload) Unmarshal(in []byte) (string, error) {
	type updateShardStatusParametersPayload struct {
		TargetStatus string `json:"targetStatus"`
	}
	var par updateShardStatusParametersPayload
	err := json.Unmarshal(in, &par)
	return par.TargetStatus, err
}

func (p updateShardStatusParamsPayload) MIME() string {
	return "vnd.weaviate.updateshardstatusparams+json"
}

func (p updateShardStatusParamsPayload) CheckContentTypeHeaderReq(r *http.Request) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}

func (p updateShardStatusParamsPayload) SetContentTypeHeaderReq(r *http.Request) {
	r.Header.Set("content-type", p.MIME())
}

type updateShardsStatusResultsPayload struct{}

func (p updateShardsStatusResultsPayload) MIME() string {
	return "application/vnd.weaviate.updateshardstatusresults+octet-stream"
}

func (p updateShardsStatusResultsPayload) SetContentTypeHeader(w http.ResponseWriter) {
	w.Header().Set("content-type", p.MIME())
}

func (p updateShardsStatusResultsPayload) CheckContentTypeHeader(r *http.Response) (string, bool) {
	ct := r.Header.Get("content-type")
	return ct, ct == p.MIME()
}
