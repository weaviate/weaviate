'use strict';
/**                         _       _       
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
     * finalize
     *
     * @param i  input URL
     * @param  weaveObject  OBJ Object with the send in body and params*/
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
                        ACTIONS.process('weave.registrationTickets.finalize', [
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
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#registrationTicket".
                             */
                            'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                            'oauthClientId',
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
                        ACTIONS.process('weave.registrationTickets.get', [
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
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#registrationTicket".
                             */
                            'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                            'oauthClientId',
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
                        ACTIONS.process('weave.registrationTickets.patch', [
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
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#registrationTicket".
                             */
                            'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                            'oauthClientId',
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
                        ACTIONS.process('weave.registrationTickets.update', [
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
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#registrationTicket".
                             */
                            'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                            'oauthClientId',
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
            ACTIONS.validateBodyObject(weaveObject, [], (result) => {
                switch (result) {
                    case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                        ACTIONS.process('weave.registrationTickets.insert', [
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
                             * type  Identifies what kind of resource this is. Value: the fixed string "weave#registrationTicket".
                             */
                            'kind',
                            /**
                             * description  string
                             * type  OAuth 2.0 client ID of the device.
                             */
                            'oauthClientId',
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