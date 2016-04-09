/*                          _       _
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
'use strict';
const ACTIONS = require('./actions.js');
module.exports = {
    /**
     * finalize
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
  finalize: (i, weaveObject, Q) => {
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
                    ACTIONS.process('clouddevices.registrationTickets.finalize', [
                            /**
                             * description  string
                             * type  Creation timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  undefined
                             * type  Draft of the device being registered.
                             */
                          'deviceDraft',
                            /**
                             * description  string
                             * type  ID that device will have after registration is successfully finished.
                             */
                          'deviceId',
                            /**
                             * description  string
                             * type  Error code. Set only on device registration failures.
                             */
                          'errorCode',
                            /**
                             * description  string
                             * type  Expiration timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Registration ticket ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#registrationTicket".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                          'oauthClientId',
                            /**
                             * description  string
                             * type  Parent device ID (aggregator) if it exists.
                             */
                          'parentId',
                            /**
                             * description  string
                             * type  Authorization code that can be exchanged to a refresh token.
                             */
                          'robotAccountAuthorizationCode',
                            /**
                             * description  string
                             * type  E-mail address of robot account assigned to the registered device.
                             */
                          'robotAccountEmail',
                            /**
                             * description  string
                             * type  Email address of the owner.
                             */
                          'userEmail'
                        ], (processResult) => {
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
                    ACTIONS.process('clouddevices.registrationTickets.get', [
                            /**
                             * description  string
                             * type  Creation timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  undefined
                             * type  Draft of the device being registered.
                             */
                          'deviceDraft',
                            /**
                             * description  string
                             * type  ID that device will have after registration is successfully finished.
                             */
                          'deviceId',
                            /**
                             * description  string
                             * type  Error code. Set only on device registration failures.
                             */
                          'errorCode',
                            /**
                             * description  string
                             * type  Expiration timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Registration ticket ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#registrationTicket".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                          'oauthClientId',
                            /**
                             * description  string
                             * type  Parent device ID (aggregator) if it exists.
                             */
                          'parentId',
                            /**
                             * description  string
                             * type  Authorization code that can be exchanged to a refresh token.
                             */
                          'robotAccountAuthorizationCode',
                            /**
                             * description  string
                             * type  E-mail address of robot account assigned to the registered device.
                             */
                          'robotAccountEmail',
                            /**
                             * description  string
                             * type  Email address of the owner.
                             */
                          'userEmail'
                        ], (processResult) => {
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
                    ACTIONS.process('clouddevices.registrationTickets.patch', [
                            /**
                             * description  string
                             * type  Creation timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  undefined
                             * type  Draft of the device being registered.
                             */
                          'deviceDraft',
                            /**
                             * description  string
                             * type  ID that device will have after registration is successfully finished.
                             */
                          'deviceId',
                            /**
                             * description  string
                             * type  Error code. Set only on device registration failures.
                             */
                          'errorCode',
                            /**
                             * description  string
                             * type  Expiration timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Registration ticket ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#registrationTicket".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                          'oauthClientId',
                            /**
                             * description  string
                             * type  Parent device ID (aggregator) if it exists.
                             */
                          'parentId',
                            /**
                             * description  string
                             * type  Authorization code that can be exchanged to a refresh token.
                             */
                          'robotAccountAuthorizationCode',
                            /**
                             * description  string
                             * type  E-mail address of robot account assigned to the registered device.
                             */
                          'robotAccountEmail',
                            /**
                             * description  string
                             * type  Email address of the owner.
                             */
                          'userEmail'
                        ], (processResult) => {
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
                    ACTIONS.process('clouddevices.registrationTickets.update', [
                            /**
                             * description  string
                             * type  Creation timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  undefined
                             * type  Draft of the device being registered.
                             */
                          'deviceDraft',
                            /**
                             * description  string
                             * type  ID that device will have after registration is successfully finished.
                             */
                          'deviceId',
                            /**
                             * description  string
                             * type  Error code. Set only on device registration failures.
                             */
                          'errorCode',
                            /**
                             * description  string
                             * type  Expiration timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Registration ticket ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#registrationTicket".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                          'oauthClientId',
                            /**
                             * description  string
                             * type  Parent device ID (aggregator) if it exists.
                             */
                          'parentId',
                            /**
                             * description  string
                             * type  Authorization code that can be exchanged to a refresh token.
                             */
                          'robotAccountAuthorizationCode',
                            /**
                             * description  string
                             * type  E-mail address of robot account assigned to the registered device.
                             */
                          'robotAccountEmail',
                            /**
                             * description  string
                             * type  Email address of the owner.
                             */
                          'userEmail'
                        ], (processResult) => {
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
                    ACTIONS.process('clouddevices.registrationTickets.insert', [
                            /**
                             * description  string
                             * type  Creation timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  undefined
                             * type  Draft of the device being registered.
                             */
                          'deviceDraft',
                            /**
                             * description  string
                             * type  ID that device will have after registration is successfully finished.
                             */
                          'deviceId',
                            /**
                             * description  string
                             * type  Error code. Set only on device registration failures.
                             */
                          'errorCode',
                            /**
                             * description  string
                             * type  Expiration timestamp of the registration ticket in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Registration ticket ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#registrationTicket".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                          'oauthClientId',
                            /**
                             * description  string
                             * type  Parent device ID (aggregator) if it exists.
                             */
                          'parentId',
                            /**
                             * description  string
                             * type  Authorization code that can be exchanged to a refresh token.
                             */
                          'robotAccountAuthorizationCode',
                            /**
                             * description  string
                             * type  E-mail address of robot account assigned to the registered device.
                             */
                          'robotAccountEmail',
                            /**
                             * description  string
                             * type  Email address of the owner.
                             */
                          'userEmail'
                        ], (processResult) => {
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
