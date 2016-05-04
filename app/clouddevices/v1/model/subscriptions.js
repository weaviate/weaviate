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
const ACTIONS = require('../controller/actions.js');
module.exports = {
    /**
     * subscribe
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    subscribe: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.subscriptions.subscribe', (processResult) => {
                        switch (processResult) {
                            case false:
                                deferred.reject('Something processing this request went wrong');
                            default:
                                deferred.resolve(processResult);
                            }
                    });
                  break;
              default: /* = FALSE */
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
          deferred.reject(error);
      }
        return deferred.promise;
    }
};
