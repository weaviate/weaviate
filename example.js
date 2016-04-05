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
 * See package.json for auther and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

/**
 * This is a local Weaviate example
 */ 
const weaviate = require('./weaviate.js');

weaviate({
	https		: false,
	https_opts	: {},
	db_hostname : 'localhost',
	db_port 	: 1000,
	db_name 	: 'test',
	db_password : 'qqq',
	hostname 	: 'localhost',
	port 	 	: '8888',
	format_in 	: 'JSON', /* use json or cbor */
	format_out 	: 'JSON', /* use json or cbor */
	stdout_log 	: true
})
.done((weaveObject) => {
	/**
	 * Weaveobject contains stuff like: params, POST body, response send back and request headers and request connection
	 */
	console.log(weaveObject);
})