'use strict';
/**                         _       _       
 *                         (_)     | |      
 *__      _____  __ ___   ___  __ _| |_ ___ 
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *                                          
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * See www.weaviate.com for details
 * See package.json for auther and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
const ACTIONS = require('./actions.js');
module.exports = {
    /**
     * list
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
    list: (i, weaveObject, Q) => {
        var deferred = Q.defer();
        try {
            /**
             * Validate if the provide body is correct
             */
            ACTIONS.validateBodyObject(weaveObject, [], (result) => {
                switch (result) {
                    case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                        ACTIONS.process('weave.events.list', [
                            /**
                             * description  array
                             * type  The actual list of events in reverse chronological order.
                             */
                            'events',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#eventsListResponse".
                             */
                            'kind',
                            /**
                             * description  string
                             * type  Token for the next page of events.
                             */
                            'nextPageToken',
                            /**
                             * description  integer
                             * type  The total number of events for the query. The number of items in a response may be smaller due to paging.
                             * format  int32
                             */
                            'totalResults'
                        ], (processResult) => {
                            switch (processResult) {
                                case false:
                                    deferred.reject('Something processing this request went wrong');
                                default:
                                    deferred.resolve(processResult);
                            }
                        });
                        break;
                    case false:
                        /**
                         * Provided body is incorrect, send error
                         */
                        deferred.reject('Provided body is incorrect');
                        break;
                }
            })
        } catch (error) {
            deferred.reject(error);
        }
        return deferred.promise;
    },
}