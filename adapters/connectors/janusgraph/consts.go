//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package janusgraph

const SCHEMA_VERSION = 1

// This file contains several constants

const KEY_VERTEX_LABEL = "_key"
const KEY_EDGE_LABEL = "_key"
const KEY_PARENT_LABEL = "keyParent"

// Shared properties for thing/action classes
const PROP_UUID = "uuid"
const PROP_KIND = "kind"
const PROP_CLASS_ID = "classId"
const PROP_REF_ID = "refId"
const PROP_AT_CONTEXT = "atContext"
const PROP_CREATION_TIME_UNIX = "creationTimeUnix"
const PROP_LAST_UPDATE_TIME_UNIX = "lastUpdateTimeUnix"

// Properties for keys.
const PROP_KEY_IS_ROOT = "keyIsRoot"
const PROP_KEY_DELETE = "keyDelete"
const PROP_KEY_EXECUTE = "keyExecute"
const PROP_KEY_READ = "keyRead"
const PROP_KEY_WRITE = "keyWrite"
const PROP_KEY_EMAIL = "keyEmail"
const PROP_KEY_IP_ORIGIN = "keyIpOrigin"
const PROP_KEY_EXPIRES_UNIX = "keyExpiresUnix"
const PROP_KEY_TOKEN = "keyToken"

// Properties for SingleRef/MultipleRef's
const PROP_REF_EDGE_CREF = "beacon"
const PROP_REF_EDGE_TYPE = "refType"
const PROP_REF_EDGE_LOCATION = "locationUrl"

// Common indices
const INDEX_BY_UUID = "byUUID"
const INDEX_BY_KIND_AND_CLASS = "byKindAndClass"
const INDEX_BY_KIND = "byKind"
const INDEX_SEARCH = "search"
