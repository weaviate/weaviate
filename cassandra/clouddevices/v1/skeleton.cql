/*                          _       _
 *                         (_)     | |
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 *
 * NOTE: Update line 21 and 22
 * NOTE: The reserved word 'token' is set as 'ttoken'
 *
 */

/**
 * Update the following lines according to your situation
 */
CREATE KEYSPACE Weaviate001 WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
USE Weaviate001;

/**
 * Do not change below this line
 */

CREATE TABLE IF NOT EXISTS AclEntry (
  cloudAccessRevoked BOOLEAN,
  creatorEmail TEXT,
  delegator TEXT,
  id UUID PRIMARY KEY,
  key BIGINT,
  kind TEXT,
  localAccessInfo TEXT,
  pending BOOLEAN,
  privileges TEXT,
  role TEXT,
  scopeId TEXT,
  scopeMembership TEXT,
  scopeName TEXT,
  scopePhotoUrl TEXT,
  scopeType TEXT
);

CREATE TABLE IF NOT EXISTS Application (
	description TEXT,
	iconUrl TEXT,
	id UUID PRIMARY KEY,
	name TEXT,
	price INT,
	publisherName TEXT,
	type TEXT,
	url TEXT
);

CREATE TABLE IF NOT EXISTS AuthorizedApp (
	androidApps TEXT,
	displayName TEXT,
	iconUrl TEXT,
	id UUID PRIMARY KEY,
	kind TEXT,
	projectId TEXT,
	ttoken TEXT // NOTE, token IS RESERVED WORD, added 't'
);

CREATE TABLE IF NOT EXISTS Command (
	blobParameters TEXT,
	blobResults TEXT,
	component TEXT,
	creationTimeMs BIGINT,
	creatorEmail TEXT,
	deviceId TEXT, 
	error TEXT,
	expirationTimeMs BIGINT,
	expirationTimeoutMs BIGINT,
	id UUID PRIMARY KEY,
	kind TEXT,
	name TEXT,
	parameters TEXT,
	progress TEXT,
	results TEXT,
	state TEXT,
	userAction TEXT
);

CREATE TABLE IF NOT EXISTS Command_queue (
	id UUID PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS Device (
	certFingerprint TEXT,
	channel TEXT, // Link to device_channel id
	commandDefs TEXT,
	components TEXT,
	connectionStatus TEXT,
	creationTimeMs BIGINT,
	description TEXT,
	deviceKind TEXT,
	deviceLocalId TEXT,
	id UUID PRIMARY KEY,
	invitations TEXT,
	isEventRecordingDisabled TEXT,
	kind TEXT,
	lastSeenTimeMs BIGINT,
	lastUpdateTimeMs BIGINT,
	lastUseTimeMs BIGINT,
	location TEXT,
	modelManifest TEXT,
	modelManifestId TEXT,
	name TEXT,
	owner TEXT,
	personalizedInfo TEXT,
	serialNumber TEXT,
	state TEXT,
	stateDefs TEXT,
	tags TEXT,
	traits TEXT,
	uiDeviceKind TEXT
);

CREATE TABLE IF NOT EXISTS Device_channel (
	connectionStatusHint TEXT,
	gcmRegistrationId TEXT,
	gcmSenderId TEXT,
	id UUID PRIMARY KEY,
	parentId TEXT,
	supportedType TEXT
);

CREATE TABLE IF NOT EXISTS DeviceStatePatchesStatePatch (
	component TEXT,
	id UUID PRIMARY KEY,
	patch TEXT,
	timeMs BIGINT
);

CREATE TABLE IF NOT EXISTS DevicesCreateLocalAuthTokensRequest (
	id UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS DevicesCreateLocalAuthTokensResponse (
	id UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS Event (
	commandPatch TEXT,
	connectionStatus TEXT,
	deviceId TEXT,
	id UUID PRIMARY KEY,
	kind TEXT,
	statePatch TEXT,
	timeMs BIGINT,
	type TEXT,
	userEmail TEXT
);

CREATE TABLE IF NOT EXISTS LocalAccessEntry (
	gaiaId BIGINT,
	id UUID PRIMARY KEY,
	isApp TEXT,
	localAccessRole TEXT,
	projectId BIGINT
);

CREATE TABLE IF NOT EXISTS LocalAuthInfo (
	id UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS Logs (
	id UUID PRIMARY KEY,
	timeMs BIGINT,
	object TEXT
)

CREATE TABLE IF NOT EXISTS MintedLocalAuthInfo (
	id UUID PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS ModelManifest (
	allowedChildModelManifestIds TEXT,
	applications TEXT,
	confirmationImageUrl TEXT,
	deviceImageUrl TEXT,
	deviceKind TEXT,
	id UUID PRIMARY KEY,
	kind TEXT,
	modelDescription TEXT,
	modelName TEXT,
	oemName TEXT,
	supportPageUrl TEXT
);

CREATE TABLE IF NOT EXISTS PackageDef (
	displayName TEXT,
	id UUID PRIMARY KEY,
	kind TEXT,
	minimalRole TEXT,
	parameters TEXT
);

CREATE TABLE IF NOT EXISTS PersonalizedInfo (
	id UUID PRIMARY KEY,
	kind TEXT,
	lastUseTimeMs BIGINT,
	location TEXT,
	name TEXT
);

CREATE TABLE IF NOT EXISTS RegistrationTicket (
	creationTimeMs BIGINT,
	deviceDraft  TEXT,
	deviceId TEXT,
	errorCode TEXT,
	expirationTimeMs BIGINT,
	id UUID PRIMARY KEY,
	kind TEXT,
	oauthClientId TEXT,
	parentId TEXT,
	robotAccountAuthorizationCode TEXT,
	robotAccountEmail TEXT,
	userEmail TEXT
);

CREATE TABLE IF NOT EXISTS StateDef (
	id UUID PRIMARY KEY,
	kind TEXT,
	minimalRole TEXT,
	name TEXT
);

CREATE TABLE IF NOT EXISTS SubscriptionData (
	expirationTimeMs BIGINT,
	filters TEXT,
	gcmRegistrationId TEXT,
	gcmSenderId TEXT,
	id UUID PRIMARY KEY,
	kind TEXT
);