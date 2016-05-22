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

const Helpers_ErrorHandling = require('Helpers/ErrorHandling.js');

/** Class Endpoints_Devices */
module.exports = class Endpoints_Devices { // Class: Commands_{resources.className}

    /**
     * Constructor for this Endpoint
     * @param {object} SERVER  - The restify SERVER object
     * @param {object} COMMAND  - The COMMAND object
     */
    constructor(SERVER, COMMAND){

        /*****************************
         * Id: weave.devices.createLocalAuthTokens
         * Creates client and device local auth tokens to be used by a client locally.
         */
        SERVER.post('/devices/createLocalAuthTokens', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getCreateLocalAuthTokens({
                        requiredParams: [],
                        requestObjectName: 'DevicesCreateLocalAuthTokensRequest',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.createLocalAuthTokens');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.createLocalAuthTokens');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.delete
         * Deletes a device from the system.
         */
        SERVER.del('/devices/:deviceId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getDelete({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: null,
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.delete');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.delete');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.get
         * Returns a particular device data.
         */
        SERVER.get('/devices/:deviceId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getGet({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: null,
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.get');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.get');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.handleInvitation
         * Returns a particular device data.
         */
        SERVER.post('/devices/:deviceId/handleInvitation', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getHandleInvitation({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }, {
                            name: 'action',
                            location: 'query'
                        }, {
                            name: 'scopeId',
                            location: 'query'
                        }],
                        requestObjectName: null,
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.handleInvitation');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.handleInvitation');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.insert
         * Registers a new device. This method may be used only by aggregator devices.
         */
        SERVER.post('/devices', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getInsert({
                        requiredParams: [],
                        requestObjectName: 'Device',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.insert');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.insert');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.list
         * Lists devices user has access to.
         */
        SERVER.get('/devices', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getList({
                        requiredParams: [],
                        requestObjectName: null,
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.list');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.list');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.patch
         * Updates a device data. This method supports patch semantics.
         */
        SERVER.patch('/devices/:deviceId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getPatch({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'Device',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.patch');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.patch');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.patchState
         * Applies provided patches to the device state. This method may be used only by devices.
         */
        SERVER.post('/devices/:deviceId/patchState', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getPatchState({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'DevicesPatchStateRequest',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.patchState');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.patchState');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.update
         * Updates a device data.
         */
        SERVER.put('/devices/:deviceId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getUpdate({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'Device',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.update');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.update');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.updateParent
         * Updates parent of the child device. Only managers can use this method.
         */
        SERVER.post('/devices/:deviceId/updateParent', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getUpdateParent({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }, {
                            name: 'parentId',
                            location: 'query'
                        }],
                        requestObjectName: 'Device',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.updateParent');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.updateParent');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.devices.upsertLocalAuthInfo
         * Upserts a device's local auth info.
         */
        SERVER.post('/devices/:deviceId/upsertLocalAuthInfo', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getUpsertLocalAuthInfo({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'DevicesUpsertLocalAuthInfoRequest',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(global.i.onSuccess !== undefined && typeof global.i.onSuccess === 'function'){
                            global.i.onSuccess({
                                params: req.params,
                                body: req.body,
                                response: result,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.devices.upsertLocalAuthInfo');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(global.i.onError !== undefined && typeof global.i.onError === 'function'){
                            global.i.onError({
                                params: req.params,
                                body: req.body,
                                requestHeaders: req.headers
                            });
                        }
                        // exec the debug
                        if(global.i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.devices.upsertLocalAuthInfo');
                        }
                        return next();
                    });
        });

     }

};
