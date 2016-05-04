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
     * cancel
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    cancel: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.commands.cancel', (processResult) => {
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
     * delete
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    delete: (i, weaveObject, Q) => {
        var deferred = Q.defer(); // no repsonse needed
        try {
            /**
             * Validate if the provide body is correct, if no body is expected, keep the array empty []
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.commands.delete', (processResult) => {
                      switch (processResult) {
                        case false:
                            deferred.reject('Something processing this request went wrong');
                        default:
                            deferred.resolve({});
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
                  ACTIONS.process('clouddevices.commands.get', (processResult) => {
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
     * patch
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    patch: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.commands.patch', (processResult) => {
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
     * update
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    update: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.commands.update', (processResult) => {
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
     * getQueue
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    getQueue: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.commands.getQueue', (processResult) => {
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
     * insert
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    insert: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.commands.insert', (processResult) => {
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
                  ACTIONS.process('clouddevices.commands.list', (processResult) => {
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
