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
     * get
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    get: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.modelManifests.get', (processResult) => {
                        switch (processResult) {
                            case false:
                                deferred.reject('Something processing this request went wrong');
                            default:
                                deferred.resolve(processResult);
                            }
                    });
                  break;
              default:
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
    },
    /**
     * list
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
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
                  ACTIONS.process('clouddevices.modelManifests.list', (processResult) => {
                        switch (processResult) {
                            case false:
                                deferred.reject('Something processing this request went wrong');
                            default:
                                deferred.resolve(processResult);
                            }
                    });
                  break;
              default:
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
    },
    /**
     * validateCommandDefs
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    validateCommandDefs: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.modelManifests.validateCommandDefs', (processResult) => {
                        switch (processResult) {
                            case false:
                                deferred.reject('Something processing this request went wrong');
                            default:
                                deferred.resolve(processResult);
                            }
                    });
                  break;
              default:
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
    },
    /**
     * validateDeviceState
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    validateDeviceState: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.modelManifests.validateDeviceState', (processResult) => {
                        switch (processResult) {
                            case false:
                                deferred.reject('Something processing this request went wrong');
                            default:
                                deferred.resolve(processResult);
                            }
                    });
                  break;
              default:
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
