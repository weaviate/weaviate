//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package esvector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/sirupsen/logrus"
)

type internalKey string

func (k internalKey) String() string {
	return string(k)
}

const (
	keyVector    internalKey = "_embedding_vector"
	keyID        internalKey = "_uuid"
	keyKind      internalKey = "_kind"
	keyClassName internalKey = "_class_name"
	keyCreated   internalKey = "_created"
	keyUpdated   internalKey = "_updated"
	keyCache     internalKey = "_cache"
	keyCacheHot  internalKey = "_hot"
)

// Repo stores and retrieves vector info in elasticsearch
type Repo struct {
	client                    *elasticsearch.Client
	logger                    logrus.FieldLogger
	schemaGetter              schemaUC.SchemaGetter
	denormalizationDepthLimit int
}

// NewRepo from existing es client
func NewRepo(client *elasticsearch.Client, logger logrus.FieldLogger,
	schemaGetter schemaUC.SchemaGetter) *Repo {
	denormalizationLimit := 3
	return &Repo{client, logger, schemaGetter, denormalizationLimit}
}

func (r *Repo) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	r.schemaGetter = sg
}

// PutThing idempotently adds a Thing with its vector representation
func (r *Repo) PutThing(ctx context.Context,
	concept *models.Thing, vector []float32) error {
	err := r.putConcept(ctx, kind.Thing, concept.ID.String(),
		concept.Class, concept.Schema, vector, concept.CreationTimeUnix,
		concept.LastUpdateTimeUnix)
	if err != nil {
		return fmt.Errorf("put thing: %v", err)
	}

	return nil
}

// PutAction idempotently adds a Action with its vector representation
func (r *Repo) PutAction(ctx context.Context,
	concept *models.Action, vector []float32) error {
	err := r.putConcept(ctx, kind.Action, concept.ID.String(),
		concept.Class, concept.Schema, vector, concept.CreationTimeUnix,
		concept.LastUpdateTimeUnix)
	if err != nil {
		return fmt.Errorf("put action: %v", err)
	}

	return nil
}

func (r *Repo) objectBucket(k kind.Kind, id, className string, props models.PropertySchema,
	vector []float32, createTime, updateTime int64) map[string]interface{} {

	bucket := map[string]interface{}{
		keyKind.String():      k.Name(),
		keyID.String():        id,
		keyClassName.String(): className,
		keyVector.String():    vectorToBase64(vector),
		keyCreated.String():   createTime,
		keyUpdated.String():   updateTime,
	}

	ex := extendBucketWithProps(bucket, props)
	return ex
}

func (r *Repo) putConcept(ctx context.Context,
	k kind.Kind, id, className string, props models.PropertySchema,
	vector []float32, createTime, updateTime int64) error {

	bucket := r.objectBucket(k, id, className, props, vector, createTime, updateTime)

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(bucket)
	if err != nil {
		return fmt.Errorf("index request: encode json: %v", err)
	}

	req := esapi.IndexRequest{
		Index:      classIndexFromClassName(k, className),
		DocumentID: id,
		Body:       &buf,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		r.logger.WithField("action", "vector_index_put_concept").
			WithError(err).
			WithField("request", req).
			WithField("res", res).
			WithField("body_before_marshal", bucket).
			WithField("body", buf.String()).
			Errorf("put concept failed")

		return fmt.Errorf("index request: %v", err)
	}

	// // TODO
	// return r.PopulateCache(ctx, k, strfmt.UUID(id))

	return nil
}

func (r *Repo) DeleteThing(ctx context.Context, className string, id strfmt.UUID) error {
	req := esapi.DeleteRequest{
		Index:      classIndexFromClassName(kind.Thing, className),
		DocumentID: id.String(),
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("delete thing: %v", err)
	}

	return nil
}

func (r *Repo) DeleteAction(ctx context.Context, className string, id strfmt.UUID) error {
	req := esapi.DeleteRequest{
		Index:      classIndexFromClassName(kind.Action, className),
		DocumentID: id.String(),
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("delete action: %v", err)
	}

	return nil
}

func extendBucketWithProps(bucket map[string]interface{}, props models.PropertySchema) map[string]interface{} {
	if props == nil {
		return bucket
	}

	hasRefs := false

	propsMap := props.(map[string]interface{})
	for key, value := range propsMap {
		if gc, ok := value.(*models.GeoCoordinates); ok {
			value = map[string]interface{}{
				"lat": gc.Latitude,
				"lon": gc.Longitude,
			}
		}

		if _, ok := value.(models.MultipleRef); ok {
			hasRefs = true
		}

		bucket[key] = value
	}

	bucket[keyCache.String()] = map[string]interface{}{
		// if a prop has Refs, it requires caching, therefore the intial state of
		// the cache is cold. However, if there are no ref props set,no caching is
		// required making the cache state hot
		keyCacheHot.String(): !hasRefs,
	}
	return bucket
}

func vectorToBase64(array []float32) string {
	bytes := make([]byte, 0, 4*len(array))
	for _, a := range array {
		bits := math.Float32bits(a)
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, bits)
		bytes = append(bytes, b...)
	}

	encoded := base64.StdEncoding.EncodeToString(bytes)
	return encoded
}

func base64ToVector(base64Str string) ([]float32, error) {
	decoded, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, err
	}

	length := len(decoded)
	array := make([]float32, 0, length/4)

	for i := 0; i < len(decoded); i += 4 {
		bits := binary.BigEndian.Uint32(decoded[i : i+4])
		f := math.Float32frombits(bits)
		array = append(array, f)
	}
	return array, nil
}

func errorResToErr(res *esapi.Response, logger logrus.FieldLogger) error {
	if !res.IsError() {
		return nil
	}

	var e map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return fmt.Errorf("request is error: status: %s", res.Status())
	}

	logger.WithField("error", e).Error("error response from es")

	return fmt.Errorf("request is error: status: %v, type: %v, reason: %v",
		res.Status(),
		e["error"].(map[string]interface{})["type"],
		e["error"].(map[string]interface{})["reason"],
	)
}
