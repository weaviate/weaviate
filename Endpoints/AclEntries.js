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

/** Class Endpoints_AclEntries */
module.exports = class Endpoints_AclEntries { // Class: Commands_{resources.className}

    /**
     * Constructor for this Endpoint
     * @param {object} SERVER  - The restify SERVER object
     * @param {object} COMMAND  - The COMMAND object
     */
    constructor(SERVER, COMMAND) {

        /*****************************
         * Id: weave.aclEntries.delete
         * Deletes an ACL entry.
         */
        SERVER.del('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getDelete({
                        requiredParams: [{
                            name: 'aclEntryId',
                            location: 'path'
                        }, {
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.delete');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.delete');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.aclEntries.get
         * Returns the requested ACL entry.
         */
        SERVER.get('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getGet({
                        requiredParams: [{
                            name: 'aclEntryId',
                            location: 'path'
                        }, {
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'AclEntry',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.get');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.get');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.aclEntries.insert
         * Inserts a new ACL entry.
         */
        SERVER.post('/devices/:deviceId/aclEntries', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getInsert({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'path'
                        }, {
                            name: 'role',
                            location: 'body'
                        }, {
                            name: 'scopeId',
                            location: 'body'
                        }],
                        requestObjectName: 'AclEntry',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.insert');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.insert');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.aclEntries.list
         * Lists ACL entries.
         */
        SERVER.get('/devices/:deviceId/aclEntries', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getList({
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.list');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.list');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.aclEntries.patch
         * Update an ACL entry. This method supports patch semantics.
         */
        SERVER.patch('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getPatch({
                        requiredParams: [{
                            name: 'aclEntryId',
                            location: 'path'
                        }, {
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'AclEntry',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.patch');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.patch');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.aclEntries.update
         * Update an ACL entry.
         */
        SERVER.put('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getUpdate({
                        requiredParams: [{
                            name: 'aclEntryId',
                            location: 'path'
                        }, {
                            name: 'deviceId',
                            location: 'path'
                        }],
                        requestObjectName: 'AclEntry',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.update');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.update');
                        }
                        return next();
                    });
        });
    }

};
