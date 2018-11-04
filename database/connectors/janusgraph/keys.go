/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package janusgraph

import (
	"context"

	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"

	"encoding/base64"
	"fmt"
	"strings"
)

//TODO fix keys cross weaviates.
func (f *Janusgraph) AddKey(ctx context.Context, key *models.Key, UUID strfmt.UUID, token string) error {
	q := gremlin.G.AddV(KEY_VERTEX_LABEL).
		StringProperty(PROP_UUID, string(UUID)).
		BoolProperty(PROP_KEY_IS_ROOT, key.Parent == nil).
		BoolProperty(PROP_KEY_DELETE, key.Delete).
		BoolProperty(PROP_KEY_EXECUTE, key.Execute).
		BoolProperty(PROP_KEY_READ, key.Read).
		BoolProperty(PROP_KEY_WRITE, key.Write).
		StringProperty(PROP_KEY_EMAIL, key.Email).
		StringProperty(PROP_KEY_IP_ORIGIN, strings.Join(key.IPOrigin, ";")).
		Int64Property(PROP_KEY_EXPIRES_UNIX, key.KeyExpiresUnix).
		StringProperty(PROP_KEY_TOKEN, base64.StdEncoding.EncodeToString([]byte(token))).
		As("newKey")

	if key.Parent != nil {
		q = q.AddE(KEY_PARENT_LABEL).
			FromRef("newKey").
			ToQuery(gremlin.G.V().
				HasLabel(KEY_VERTEX_LABEL).
				HasString(PROP_UUID, key.Parent.NrDollarCref.String()))
	}

	_, err := f.client.Execute(q)

	return err
}

func (f *Janusgraph) GetKey(ctx context.Context, UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error {
	q := gremlin.G.V().HasLabel(KEY_VERTEX_LABEL).HasString(PROP_UUID, string(UUID))

	result, err := f.client.Execute(q)

	if err != nil {
		return err
	}

	vertices, err := result.Vertices()

	if err != nil {
		return err
	}

	if len(vertices) == 0 {
		return fmt.Errorf("No key found")
	}

	if len(vertices) != 1 {
		return fmt.Errorf("More than one key with UUID '%v' found!", UUID)
	}

	vertex := vertices[0]
	fillKeyResponseFromVertex(&vertex, keyResponse)

	return nil
}

func (f *Janusgraph) GetKeys(ctx context.Context, UUIDs []strfmt.UUID, keysResponse *[]*models.KeyGetResponse) error {
	for _, id := range UUIDs {
		var response *models.KeyGetResponse = new(models.KeyGetResponse)
		err := f.GetKey(ctx, id, response)
		if err != nil {
			return err
		}
		*keysResponse = append(*keysResponse, response)
	}

	return nil
}

func (f *Janusgraph) DeleteKey(ctx context.Context, key *models.Key, UUID strfmt.UUID) error {
	q := gremlin.G.V().HasLabel(KEY_VERTEX_LABEL).
		HasString(PROP_UUID, string(UUID)).Drop()

	_, err := f.client.Execute(q)

	return err
}

// GetKeyChildren fills the given KeyGetResponse array with the values from the database, based on the given UUID.
func (f *Janusgraph) GetKeyChildren(ctx context.Context, UUID strfmt.UUID, children *[]*models.KeyGetResponse) error {
	// Fetch the child vertices directly, so that we can run just _one_ query instead of 1 + len(children)
	q := gremlin.G.V().HasLabel(KEY_VERTEX_LABEL).HasString(PROP_UUID, string(UUID)).InEWithLabel(KEY_PARENT_LABEL).OutV()

	result, err := f.client.Execute(q)
	if err != nil {
		return err
	}
	vertices, err := result.Vertices()
	if err != nil {
		return err
	}

	for _, vertex := range vertices {
		child := models.KeyGetResponse{}
		fillKeyResponseFromVertex(&vertex, &child)
		*children = append(*children, &child)
	}

	return nil
}

// UpdateKey updates the Key in the DB at the given UUID.
func (f *Janusgraph) UpdateKey(ctx context.Context, key *models.Key, UUID strfmt.UUID, token string) error {
	q := gremlin.G.V().HasLabel(KEY_VERTEX_LABEL).
		HasString(PROP_UUID, string(UUID)).
		BoolProperty(PROP_KEY_IS_ROOT, key.Parent == nil).
		BoolProperty(PROP_KEY_DELETE, key.Delete).
		BoolProperty(PROP_KEY_EXECUTE, key.Execute).
		BoolProperty(PROP_KEY_READ, key.Read).
		BoolProperty(PROP_KEY_WRITE, key.Write).
		StringProperty(PROP_KEY_EMAIL, key.Email).
		StringProperty(PROP_KEY_IP_ORIGIN, strings.Join(key.IPOrigin, ";")).
		Int64Property(PROP_KEY_EXPIRES_UNIX, key.KeyExpiresUnix).
		StringProperty(PROP_KEY_TOKEN, base64.StdEncoding.EncodeToString([]byte(token)))

	_, err := f.client.Execute(q)

	return err
}
