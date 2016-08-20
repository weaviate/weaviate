'use strict';
/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

/** Class Commands_Events */
module.exports = class Commands_Events { // Class: Commands_{resources.className}

    /**
     * Constructor for this Command
     * @param {object} req  - The request
     * @param {object} res  - The response
     * @param {object} next - Next() function
     */
    constructor(req, res, next) {
        this.req  = req;
        this.res  = res;
        this.next = next;
    }

    /**
     * Deletes all events associated with a particular device. Leaves an event to indicate deletion happened.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $DeleteAll(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({});
        });
    }

    /**
     * Lists events.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $List(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
              kind: 'weave#eventsListResponse',
              events: [
                {
                commandPatch: {
                        commandId: 'qwerty-12345',
                        state: 'state'
                },
                connectionStatus: 'connection status',
                deviceId: 'qwerty-12345',
                id: '12345-qwerty',
                kind: 'weave#event',
                statePatch: {},
                timeMs: 12345,
                type: 'commandCreated',
                userEmail: 'bob@weaviate.com'
            }
              ],
              nextPageToken: '12345',
              totalResults: 1
            });
        });
    }

    /**
     * Enables or disables recording of a particular device's events based on a boolean parameter. Enabled by default.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $RecordDeviceEvents(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({});
        });
    }

};
