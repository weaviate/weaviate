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
     * createLocalAuthTokens
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    createLocalAuthTokens: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.devices.createLocalAuthTokens', (processResult) => {
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
                  ACTIONS.process('clouddevices.devices.delete', (processResult) => {
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
                  ACTIONS.process('clouddevices.devices.get', (processResult) => {
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
                  ACTIONS.process('clouddevices.devices.patch', (processResult) => {
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
                  ACTIONS.process('clouddevices.devices.update', (processResult) => {
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
     * handleInvitation
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    handleInvitation: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.devices.handleInvitation', (processResult) => {
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
                  ACTIONS.process('clouddevices.devices.insert', (processResult) => {
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
                  ACTIONS.process('clouddevices.devices.list', (processResult) => {
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
     * patchState
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    patchState: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.devices.patchState', (processResult) => {
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
     * updateParent
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    updateParent: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.devices.updateParent', (processResult) => {
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
     * upsertLocalAuthInfo
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    upsertLocalAuthInfo: (i, weaveObject, Q) => {
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
                  ACTIONS.process('clouddevices.devices.upsertLocalAuthInfo', (processResult) => {
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
