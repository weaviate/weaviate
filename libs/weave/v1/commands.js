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
     * cancel
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                    ACTIONS.process('weave.commands.cancel', [
                            /**
                             * description  string
                             * type  Timestamp since epoch of a creation of a command.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User that created the command (not applicable if the user is deleted).
                             */
                          'creatorEmail',
                            /**
                             * description  string
                             * type  Device ID that this command belongs to.
                             * required  weave.commands.insert
                             */
                          'deviceId',
                            /**
                             * description  object
                             * type  Error descriptor.
                             */
                          'error',
                            /**
                             * description  string
                             * type  Timestamp since epoch of command expiration.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
                             * format  int64
                             */
                          'expirationTimeoutMs',
                            /**
                             * description  string
                             * type  Unique command ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#command".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Full command name, including package.
                             * required  weave.commands.insert
                             */
                          'name',
                            /**
                             * description  undefined
                             * type  Parameters list.
                             */
                          'parameters',
                            /**
                             * description  undefined
                             * type  Command-specific progress descriptor.
                             */
                          'progress',
                            /**
                             * description  undefined
                             * type  Results list.
                             */
                          'results',
                            /**
                             * description  string
                             * type  Current command state.
                             * enum  aborted, cancelled, done, error, expired, inProgress, queued
                             */
                          'state',
                            /**
                             * description  string
                             * type  Pending command state that is not acknowledged by the device yet.
                             */
                          'userAction'
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
    /**
     * delete
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                    ACTIONS.process('weave.commands.delete', [], (processResult) => {
                          switch (processResult) {
                              case false:
                                deferred.reject('Something processing this request went wrong');
                              default:
                                deferred.resolve({});
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
    /**
     * get
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                    ACTIONS.process('weave.commands.get', [
                            /**
                             * description  string
                             * type  Timestamp since epoch of a creation of a command.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User that created the command (not applicable if the user is deleted).
                             */
                          'creatorEmail',
                            /**
                             * description  string
                             * type  Device ID that this command belongs to.
                             * required  weave.commands.insert
                             */
                          'deviceId',
                            /**
                             * description  object
                             * type  Error descriptor.
                             */
                          'error',
                            /**
                             * description  string
                             * type  Timestamp since epoch of command expiration.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
                             * format  int64
                             */
                          'expirationTimeoutMs',
                            /**
                             * description  string
                             * type  Unique command ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#command".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Full command name, including package.
                             * required  weave.commands.insert
                             */
                          'name',
                            /**
                             * description  undefined
                             * type  Parameters list.
                             */
                          'parameters',
                            /**
                             * description  undefined
                             * type  Command-specific progress descriptor.
                             */
                          'progress',
                            /**
                             * description  undefined
                             * type  Results list.
                             */
                          'results',
                            /**
                             * description  string
                             * type  Current command state.
                             * enum  aborted, cancelled, done, error, expired, inProgress, queued
                             */
                          'state',
                            /**
                             * description  string
                             * type  Pending command state that is not acknowledged by the device yet.
                             */
                          'userAction'
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
    /**
     * patch
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                    ACTIONS.process('weave.commands.patch', [
                            /**
                             * description  string
                             * type  Timestamp since epoch of a creation of a command.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User that created the command (not applicable if the user is deleted).
                             */
                          'creatorEmail',
                            /**
                             * description  string
                             * type  Device ID that this command belongs to.
                             * required  weave.commands.insert
                             */
                          'deviceId',
                            /**
                             * description  object
                             * type  Error descriptor.
                             */
                          'error',
                            /**
                             * description  string
                             * type  Timestamp since epoch of command expiration.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
                             * format  int64
                             */
                          'expirationTimeoutMs',
                            /**
                             * description  string
                             * type  Unique command ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#command".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Full command name, including package.
                             * required  weave.commands.insert
                             */
                          'name',
                            /**
                             * description  undefined
                             * type  Parameters list.
                             */
                          'parameters',
                            /**
                             * description  undefined
                             * type  Command-specific progress descriptor.
                             */
                          'progress',
                            /**
                             * description  undefined
                             * type  Results list.
                             */
                          'results',
                            /**
                             * description  string
                             * type  Current command state.
                             * enum  aborted, cancelled, done, error, expired, inProgress, queued
                             */
                          'state',
                            /**
                             * description  string
                             * type  Pending command state that is not acknowledged by the device yet.
                             */
                          'userAction'
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
    /**
     * update
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                    ACTIONS.process('weave.commands.update', [
                            /**
                             * description  string
                             * type  Timestamp since epoch of a creation of a command.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User that created the command (not applicable if the user is deleted).
                             */
                          'creatorEmail',
                            /**
                             * description  string
                             * type  Device ID that this command belongs to.
                             * required  weave.commands.insert
                             */
                          'deviceId',
                            /**
                             * description  object
                             * type  Error descriptor.
                             */
                          'error',
                            /**
                             * description  string
                             * type  Timestamp since epoch of command expiration.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
                             * format  int64
                             */
                          'expirationTimeoutMs',
                            /**
                             * description  string
                             * type  Unique command ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#command".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Full command name, including package.
                             * required  weave.commands.insert
                             */
                          'name',
                            /**
                             * description  undefined
                             * type  Parameters list.
                             */
                          'parameters',
                            /**
                             * description  undefined
                             * type  Command-specific progress descriptor.
                             */
                          'progress',
                            /**
                             * description  undefined
                             * type  Results list.
                             */
                          'results',
                            /**
                             * description  string
                             * type  Current command state.
                             * enum  aborted, cancelled, done, error, expired, inProgress, queued
                             */
                          'state',
                            /**
                             * description  string
                             * type  Pending command state that is not acknowledged by the device yet.
                             */
                          'userAction'
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
    /**
     * getQueue
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                    ACTIONS.process('weave.commands.getQueue', [
                            /**
                             * description  array
                             * type  Commands to be executed.
                             */
                          'commands'
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
    /**
     * insert
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
  insert: (i, weaveObject, Q) => {
      var deferred = Q.defer();
      try {
            /**
             * Validate if the provide body is correct
             */
          ACTIONS.validateBodyObject(weaveObject, ['deviceId', 'name'], (result) => {
              switch (result) {
                  case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                    ACTIONS.process('weave.commands.insert', [
                            /**
                             * description  string
                             * type  Timestamp since epoch of a creation of a command.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User that created the command (not applicable if the user is deleted).
                             */
                          'creatorEmail',
                            /**
                             * description  string
                             * type  Device ID that this command belongs to.
                             * required  weave.commands.insert
                             */
                          'deviceId',
                            /**
                             * description  object
                             * type  Error descriptor.
                             */
                          'error',
                            /**
                             * description  string
                             * type  Timestamp since epoch of command expiration.
                             * format  int64
                             */
                          'expirationTimeMs',
                            /**
                             * description  string
                             * type  Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
                             * format  int64
                             */
                          'expirationTimeoutMs',
                            /**
                             * description  string
                             * type  Unique command ID.
                             */
                          'id',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#command".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Full command name, including package.
                             * required  weave.commands.insert
                             */
                          'name',
                            /**
                             * description  undefined
                             * type  Parameters list.
                             */
                          'parameters',
                            /**
                             * description  undefined
                             * type  Command-specific progress descriptor.
                             */
                          'progress',
                            /**
                             * description  undefined
                             * type  Results list.
                             */
                          'results',
                            /**
                             * description  string
                             * type  Current command state.
                             * enum  aborted, cancelled, done, error, expired, inProgress, queued
                             */
                          'state',
                            /**
                             * description  string
                             * type  Pending command state that is not acknowledged by the device yet.
                             */
                          'userAction'
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
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
                    ACTIONS.process('weave.commands.list', [
                            /**
                             * description  array
                             * type  The actual list of commands.
                             */
                          'commands',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#commandsListResponse".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Token for the next page of commands.
                             */
                          'nextPageToken',
                            /**
                             * description  integer
                             * type  The total number of commands for the query. The number of items in a response may be smaller due to paging.
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
            });
        } catch (error) {
          deferred.reject(error);
        }
      return deferred.promise;
    },
};