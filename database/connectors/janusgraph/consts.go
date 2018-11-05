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

// Common indices
const INDEX_BY_UUID = "byUUID"
const INDEX_BY_KIND_AND_CLASS = "byKindAndClass"
