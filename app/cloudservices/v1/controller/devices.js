'use strict';
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
 */

module.exports = {

    /**
     * List all devices
     * returns devices,kind,totalResults
     * @param {Array}  returnValuesArray  Array of values that will be returned
     * @param {Object} callback           Callback is the callback, return true if all fields are included, returns false if not
     * @returns {void} nothing
     */
    list: (returnValuesArray, callback) => {

            var deviceNames  = [],
                query = 'SELECT deviceKind FROM devices';

            GLOBAL.CLIENT.execute(query, [], { prepare: true }, (err, result) => {

                /**
                 * Create device name array
                 */
                result.rows.forEach((val) => {
                    deviceNames.push(val.devicekind);
                });

                /**
                 * Send everything back with the callback
                 */
                callback({
                  'kind': 'weave#devicesListResponse',
                  'devices': deviceNames,
                  'nextPageToken': 'EMPTY',
                  'totalResults': result.rowLength
                });
            });
    }
};
