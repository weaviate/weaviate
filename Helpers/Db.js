'use strict';
/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

/**
 * CLASS Helpers_Db
 */
module.exports = class Helpers_Db {

    creatUid() {
        return require('cassandra-driver').types.Uuid.random();
    }

    createQueryInsert(table, i) {
    	var keys  	= [],
    		values	= [];

    	for (var key in i) {
		    if (!i.hasOwnProperty(key)) continue;
		    keys.push(key);
		    if (i[key] !== '?') i[key] = "'" + i[key] + "'"; 
		    values.push(i[key]);
		}

		return 'INSERT INTO ' + table + ' (' + keys.join() + ') VALUES (' + values.join() + ');';
    }

};
