'use strict';
/**                         _       _
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
 */

const weaviate = require('./weaviate.js');

weaviate({
	https: false,
	httpsOpts: {},
	dbHostname: 'localhost',
	dbPort: 1000,
	dbName: 'test',
	dbPassword: 'qqq',
	hostname: '0.0.0.0',
	port: '80',
	formatIn: 'JSON', /* use json or cbor */
	formatOut: 'JSON', /* use json or cbor */
	stdoutLog: true
}).done((weaveObject) => {
	console.log(weaveObject);
});
