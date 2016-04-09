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
 * See package.json for auther and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
const ACTIONS = require('./actions.js');
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
                    ACTIONS.process('clouddevices.devices.createLocalAuthTokens', [
                            /**
                             * description  array
                             * type  Minted device and client tokens.
                             */
                          'mintedLocalAuthTokens'
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
                    ACTIONS.process('clouddevices.devices.delete', [], (processResult) => {
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
                    ACTIONS.process('clouddevices.devices.get', [
                            /**
                             * description  string
                             * type  The HTTPS certificate fingerprint used to secure communication with device..
                             */
                          'certFingerprint',
                            /**
                             * description  object
                             * type  Device notification channel description.
                             * required  clouddevices.devices.insert
                             */
                          'channel',
                            /**
                             * description  object
                             * type  Description of commands supported by the device. This field is writable only by devices.
                             * required  clouddevices.devices.insert
                             */
                          'commandDefs',
                            /**
                             * description  undefined
                             * type  Hierarchical componenet-based modeling of the device.
                             */
                          'components',
                            /**
                             * description  string
                             * type  Device connection status.
                             */
                          'connectionStatus',
                            /**
                             * description  string
                             * type  Timestamp of creation of this device in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User readable description of this device.
                             */
                          'description',
                            /**
                             * description  string
                             * type  Device kind. Deprecated, provide "modelManifestId" instead.
                             * enum  accessPoint, aggregator, camera, developmentBoard, lock, printer, scanner, speaker, storage, toy, vendor, video
                             */
                          'deviceKind',
                            /**
                             * description  string
                             * type  The ID of the device for use on the local network.
                             */
                          'deviceLocalId',
                            /**
                             * description  string
                             * type  Unique device ID.
                             */
                          'id',
                            /**
                             * description  array
                             * type  List of pending invitations for the currently logged-in user.
                             */
                          'invitations',
                            /**
                             * description  boolean
                             * type  Indicates whether event recording is enabled or disabled for this device.
                             */
                          'isEventRecordingDisabled',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#device".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.
                             * format  int64
                             */
                          'lastSeenTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device update in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUpdateTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device usage in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUseTimeMs',
                            /**
                             * description  string
                             * type  User readable location of the device (name of the room, office number, building/floor, etc).
                             */
                          'location',
                            /**
                             * description  object
                             * type  Device model information provided by the model manifest of this device.
                             */
                          'modelManifest',
                            /**
                             * description  string
                             * type  Model manifest ID of this device.
                             */
                          'modelManifestId',
                            /**
                             * description  string
                             * type  Name of this device provided by the manufacturer.
                             */
                          'name',
                            /**
                             * description  string
                             * type  E-mail address of the device owner.
                             */
                          'owner',
                            /**
                             * description  object
                             * type  Personalized device information for currently logged-in user.
                             */
                          'personalizedInfo',
                            /**
                             * description  string
                             * type  Serial number of a device provided by its manufacturer.
                             * required  clouddevices.devices.insert
                             */
                          'serialNumber',
                            /**
                             * description  undefined
                             * type  Device state. This field is writable only by devices.
                             */
                          'state',
                            /**
                             * description  object
                             * type  Description of the device state. This field is writable only by devices.
                             */
                          'stateDefs',
                            /**
                             * description  array
                             * type  Custom free-form manufacturer tags.
                             */
                          'tags',
                            /**
                             * description  undefined
                             * type  Traits defined for the device.
                             */
                          'traits',
                            /**
                             * description  string
                             * type  Device kind from the model manifest used in UI applications.
                             */
                          'uiDeviceKind'
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
                    ACTIONS.process('clouddevices.devices.patch', [
                            /**
                             * description  string
                             * type  The HTTPS certificate fingerprint used to secure communication with device..
                             */
                          'certFingerprint',
                            /**
                             * description  object
                             * type  Device notification channel description.
                             * required  clouddevices.devices.insert
                             */
                          'channel',
                            /**
                             * description  object
                             * type  Description of commands supported by the device. This field is writable only by devices.
                             * required  clouddevices.devices.insert
                             */
                          'commandDefs',
                            /**
                             * description  undefined
                             * type  Hierarchical componenet-based modeling of the device.
                             */
                          'components',
                            /**
                             * description  string
                             * type  Device connection status.
                             */
                          'connectionStatus',
                            /**
                             * description  string
                             * type  Timestamp of creation of this device in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User readable description of this device.
                             */
                          'description',
                            /**
                             * description  string
                             * type  Device kind. Deprecated, provide "modelManifestId" instead.
                             * enum  accessPoint, aggregator, camera, developmentBoard, lock, printer, scanner, speaker, storage, toy, vendor, video
                             */
                          'deviceKind',
                            /**
                             * description  string
                             * type  The ID of the device for use on the local network.
                             */
                          'deviceLocalId',
                            /**
                             * description  string
                             * type  Unique device ID.
                             */
                          'id',
                            /**
                             * description  array
                             * type  List of pending invitations for the currently logged-in user.
                             */
                          'invitations',
                            /**
                             * description  boolean
                             * type  Indicates whether event recording is enabled or disabled for this device.
                             */
                          'isEventRecordingDisabled',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#device".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.
                             * format  int64
                             */
                          'lastSeenTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device update in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUpdateTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device usage in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUseTimeMs',
                            /**
                             * description  string
                             * type  User readable location of the device (name of the room, office number, building/floor, etc).
                             */
                          'location',
                            /**
                             * description  object
                             * type  Device model information provided by the model manifest of this device.
                             */
                          'modelManifest',
                            /**
                             * description  string
                             * type  Model manifest ID of this device.
                             */
                          'modelManifestId',
                            /**
                             * description  string
                             * type  Name of this device provided by the manufacturer.
                             */
                          'name',
                            /**
                             * description  string
                             * type  E-mail address of the device owner.
                             */
                          'owner',
                            /**
                             * description  object
                             * type  Personalized device information for currently logged-in user.
                             */
                          'personalizedInfo',
                            /**
                             * description  string
                             * type  Serial number of a device provided by its manufacturer.
                             * required  clouddevices.devices.insert
                             */
                          'serialNumber',
                            /**
                             * description  undefined
                             * type  Device state. This field is writable only by devices.
                             */
                          'state',
                            /**
                             * description  object
                             * type  Description of the device state. This field is writable only by devices.
                             */
                          'stateDefs',
                            /**
                             * description  array
                             * type  Custom free-form manufacturer tags.
                             */
                          'tags',
                            /**
                             * description  undefined
                             * type  Traits defined for the device.
                             */
                          'traits',
                            /**
                             * description  string
                             * type  Device kind from the model manifest used in UI applications.
                             */
                          'uiDeviceKind'
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
                    ACTIONS.process('clouddevices.devices.update', [
                            /**
                             * description  string
                             * type  The HTTPS certificate fingerprint used to secure communication with device..
                             */
                          'certFingerprint',
                            /**
                             * description  object
                             * type  Device notification channel description.
                             * required  clouddevices.devices.insert
                             */
                          'channel',
                            /**
                             * description  object
                             * type  Description of commands supported by the device. This field is writable only by devices.
                             * required  clouddevices.devices.insert
                             */
                          'commandDefs',
                            /**
                             * description  undefined
                             * type  Hierarchical componenet-based modeling of the device.
                             */
                          'components',
                            /**
                             * description  string
                             * type  Device connection status.
                             */
                          'connectionStatus',
                            /**
                             * description  string
                             * type  Timestamp of creation of this device in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User readable description of this device.
                             */
                          'description',
                            /**
                             * description  string
                             * type  Device kind. Deprecated, provide "modelManifestId" instead.
                             * enum  accessPoint, aggregator, camera, developmentBoard, lock, printer, scanner, speaker, storage, toy, vendor, video
                             */
                          'deviceKind',
                            /**
                             * description  string
                             * type  The ID of the device for use on the local network.
                             */
                          'deviceLocalId',
                            /**
                             * description  string
                             * type  Unique device ID.
                             */
                          'id',
                            /**
                             * description  array
                             * type  List of pending invitations for the currently logged-in user.
                             */
                          'invitations',
                            /**
                             * description  boolean
                             * type  Indicates whether event recording is enabled or disabled for this device.
                             */
                          'isEventRecordingDisabled',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#device".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.
                             * format  int64
                             */
                          'lastSeenTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device update in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUpdateTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device usage in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUseTimeMs',
                            /**
                             * description  string
                             * type  User readable location of the device (name of the room, office number, building/floor, etc).
                             */
                          'location',
                            /**
                             * description  object
                             * type  Device model information provided by the model manifest of this device.
                             */
                          'modelManifest',
                            /**
                             * description  string
                             * type  Model manifest ID of this device.
                             */
                          'modelManifestId',
                            /**
                             * description  string
                             * type  Name of this device provided by the manufacturer.
                             */
                          'name',
                            /**
                             * description  string
                             * type  E-mail address of the device owner.
                             */
                          'owner',
                            /**
                             * description  object
                             * type  Personalized device information for currently logged-in user.
                             */
                          'personalizedInfo',
                            /**
                             * description  string
                             * type  Serial number of a device provided by its manufacturer.
                             * required  clouddevices.devices.insert
                             */
                          'serialNumber',
                            /**
                             * description  undefined
                             * type  Device state. This field is writable only by devices.
                             */
                          'state',
                            /**
                             * description  object
                             * type  Description of the device state. This field is writable only by devices.
                             */
                          'stateDefs',
                            /**
                             * description  array
                             * type  Custom free-form manufacturer tags.
                             */
                          'tags',
                            /**
                             * description  undefined
                             * type  Traits defined for the device.
                             */
                          'traits',
                            /**
                             * description  string
                             * type  Device kind from the model manifest used in UI applications.
                             */
                          'uiDeviceKind'
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
                    ACTIONS.process('clouddevices.devices.handleInvitation', [], (processResult) => {
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
                    ACTIONS.process('clouddevices.devices.insert', [
                            /**
                             * description  string
                             * type  The HTTPS certificate fingerprint used to secure communication with device..
                             */
                          'certFingerprint',
                            /**
                             * description  object
                             * type  Device notification channel description.
                             * required  clouddevices.devices.insert
                             */
                          'channel',
                            /**
                             * description  object
                             * type  Description of commands supported by the device. This field is writable only by devices.
                             * required  clouddevices.devices.insert
                             */
                          'commandDefs',
                            /**
                             * description  undefined
                             * type  Hierarchical componenet-based modeling of the device.
                             */
                          'components',
                            /**
                             * description  string
                             * type  Device connection status.
                             */
                          'connectionStatus',
                            /**
                             * description  string
                             * type  Timestamp of creation of this device in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'creationTimeMs',
                            /**
                             * description  string
                             * type  User readable description of this device.
                             */
                          'description',
                            /**
                             * description  string
                             * type  Device kind. Deprecated, provide "modelManifestId" instead.
                             * enum  accessPoint, aggregator, camera, developmentBoard, lock, printer, scanner, speaker, storage, toy, vendor, video
                             */
                          'deviceKind',
                            /**
                             * description  string
                             * type  The ID of the device for use on the local network.
                             */
                          'deviceLocalId',
                            /**
                             * description  string
                             * type  Unique device ID.
                             */
                          'id',
                            /**
                             * description  array
                             * type  List of pending invitations for the currently logged-in user.
                             */
                          'invitations',
                            /**
                             * description  boolean
                             * type  Indicates whether event recording is enabled or disabled for this device.
                             */
                          'isEventRecordingDisabled',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#device".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.
                             * format  int64
                             */
                          'lastSeenTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device update in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUpdateTimeMs',
                            /**
                             * description  string
                             * type  Timestamp of the last device usage in milliseconds since epoch UTC.
                             * format  int64
                             */
                          'lastUseTimeMs',
                            /**
                             * description  string
                             * type  User readable location of the device (name of the room, office number, building/floor, etc).
                             */
                          'location',
                            /**
                             * description  object
                             * type  Device model information provided by the model manifest of this device.
                             */
                          'modelManifest',
                            /**
                             * description  string
                             * type  Model manifest ID of this device.
                             */
                          'modelManifestId',
                            /**
                             * description  string
                             * type  Name of this device provided by the manufacturer.
                             */
                          'name',
                            /**
                             * description  string
                             * type  E-mail address of the device owner.
                             */
                          'owner',
                            /**
                             * description  object
                             * type  Personalized device information for currently logged-in user.
                             */
                          'personalizedInfo',
                            /**
                             * description  string
                             * type  Serial number of a device provided by its manufacturer.
                             * required  clouddevices.devices.insert
                             */
                          'serialNumber',
                            /**
                             * description  undefined
                             * type  Device state. This field is writable only by devices.
                             */
                          'state',
                            /**
                             * description  object
                             * type  Description of the device state. This field is writable only by devices.
                             */
                          'stateDefs',
                            /**
                             * description  array
                             * type  Custom free-form manufacturer tags.
                             */
                          'tags',
                            /**
                             * description  undefined
                             * type  Traits defined for the device.
                             */
                          'traits',
                            /**
                             * description  string
                             * type  Device kind from the model manifest used in UI applications.
                             */
                          'uiDeviceKind'
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
                    ACTIONS.process('clouddevices.devices.list', [
                            /**
                             * description  array
                             * type  The actual list of devices.
                             */
                          'devices',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#devicesListResponse".
                             */
                          'kind',
                            /**
                             * description  string
                             * type  Token corresponding to the next page of devices.
                             */
                          'nextPageToken',
                            /**
                             * description  integer
                             * type  The total number of devices for the query. The number of items in a response may be smaller due to paging.
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
                    ACTIONS.process('clouddevices.devices.patchState', [
                            /**
                             * description  undefined
                             * type  Updated device state.
                             */
                          'state'
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
                    ACTIONS.process('clouddevices.devices.updateParent', [], (processResult) => {
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
                    ACTIONS.process('clouddevices.devices.upsertLocalAuthInfo', [
                            /**
                             * description  undefined
                             * type  The non-secret local auth info.
                             */
                          'localAuthInfo'
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
