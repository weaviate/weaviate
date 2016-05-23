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

/** Class Endpoints_RegistrationTickets */
module.exports = class Endpoints_RegistrationTickets { // Class: Commands_{resources.className}

    /**
     * Constructor for this Endpoint
     * @param {object} SERVER  - The restify SERVER object
     * @param {object} COMMAND  - The COMMAND object
     */
    constructor(SERVER, COMMAND){

        /*****************************
         * Id: weave.registrationTickets.finalize
         * Finalizes device registration and returns its credentials. This method may be used only by devices.
         */
        SERVER.post('/registrationTickets/:registrationTicketId/finalize', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getFinalize({
                        requiredParams: [{
                            name: 'registrationTicketId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.registrationTickets.finalize');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.registrationTickets.finalize');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.registrationTickets.get
         * Returns an existing registration ticket.
         */
        SERVER.get('/registrationTickets/:registrationTicketId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getGet({
                        requiredParams: [{
                            name: 'registrationTicketId',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.registrationTickets.get');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.registrationTickets.get');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.registrationTickets.insert
         * Creates a new registration ticket.
         */
        SERVER.post('/registrationTickets', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getInsert({
                        requiredParams: [],
                        requestObjectName: 'RegistrationTicket',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.registrationTickets.insert');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.registrationTickets.insert');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.registrationTickets.patch
         * Updates an existing registration ticket. This method supports patch semantics.
         */
        SERVER.patch('/registrationTickets/:registrationTicketId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getPatch({
                        requiredParams: [{
                            name: 'registrationTicketId',
                            loaction: 'path'
                        }],
                        requestObjectName: 'RegistrationTicket',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.registrationTickets.patch');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.registrationTickets.patch');
                        }
                        return next();
                    });
        });

        /*****************************
         * Id: weave.registrationTickets.update
         * Updates an existing registration ticket.
         */
        SERVER.put('/registrationTickets/:registrationTicketId', (req, res, next) => {
            return new COMMAND(req, res, next)
                    .getUpdate({
                        requiredParams: [{
                            name: 'registrationTicketId',
                            loaction: 'path'
                        }],
                        requestObjectName: 'RegistrationTicket',
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
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.registrationTickets.update');
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
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.registrationTickets.update');
                        }
                        return next();
                    });
        });

    }

};
