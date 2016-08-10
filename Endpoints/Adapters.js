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

const Helpers_ErrorHandling = require('../Helpers/ErrorHandling.js');

/** Class Endpoints_Adapters */
module.exports = class Endpoints_Adapters { // Class: Commands_{resources.className}

    /**
     * Constructor for this Endpoint
     * @param {object} SERVER  - The restify SERVER object
     * @param {object} COMMAND  - The COMMAND object
     */
    constructor(SERVER, COMMAND) {

        /*****************************
         * Id: weave.adapter.accept
         * Accept an Adapter.
         */
        SERVER.post('/adapters/accept', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getAccept({
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.adapter.accept');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.adapter.accept');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.adapter.activate
         * Activates an Adapter.
         */
        SERVER.post('/adapters/:adapterId/activate', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getActivate({
                        requiredParams: [{
                            name: 'adapterId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.adapter.activate');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.adapter.activate');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.adapter.deactivate
         * Deactivates an Adapter.
         */
        SERVER.post('/adapters/:adapterId/deactivate', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getDeactivate({
                        requiredParams: [{
                            name: 'adapterId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.adapter.deactivate');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.adapter.deactivate');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.adapter.get
         * Get an Adapter.
         */
        SERVER.get('/adapters/:adapterId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getGet({
                        requiredParams: [{
                            name: 'adapterId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.adapter.get');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.adapter.get');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.adapter.list
         * Lists all Adapters.
         */
        SERVER.get('/adapters', (req, res, next) => {
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.adapter.list');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.adapter.list');
                        }
                        return next();
                    });
        });

    };

};
