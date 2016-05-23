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

/** Class Endpoints_Places */
module.exports = class Endpoints_Places { // Class: Commands_{resources.className}

    /**
     * Constructor for this Endpoint
     * @param {object} SERVER  - The restify SERVER object
     * @param {object} COMMAND  - The COMMAND object
     */
    constructor(SERVER, COMMAND){

        /*****************************
         * Id: weave.places.delete
         * Deletes a place.
         */
        SERVER.del('/places/:placeId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getDelete({
                        requiredParams: [{
                            name: 'placeId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.places.delete');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.places.delete');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.places.get
         * Returns a particular place data.
         */
        SERVER.get('/places/:placeId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getGet({
                        requiredParams: [{
                            name: 'placeId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.places.get');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.places.get');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.places.insert
         * Registers a new place.
         */
        SERVER.post('/places', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getInsert({
                        requiredParams: [],
                        requestObjectName: 'Place',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.places.insert');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.places.insert');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.places.list
         * Lists user's places (homes).
         */
        SERVER.get('/places', (req, res, next) => {
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.places.list');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.places.list');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.places.patch
         * Updates a place. This method supports patch semantics.
         */
        SERVER.patch('/places/:placeId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getPatch({
                        requiredParams: [{
                            name: 'placeId',
                            location: 'path'
                        }],
                        requestObjectName: 'Place',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.places.patch');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.places.patch');
                        }
                        return next();
                    });
        });

           /*****************************
         * Id: weave.places.update
         * Updates a place.
         */
        SERVER.put('/places/:placeId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getUpdate({
                        requiredParams: [{
                            name: 'placeId',
                            location: 'path'
                        }],
                        requestObjectName: 'Place',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.places.update');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.places.update');
                        }
                        return next();
                    });
        });

    }

};
