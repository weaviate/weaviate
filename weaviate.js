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

const Helpers_ErrorHandling = require('./Helpers/ErrorHandling.js');

const Commands_AclEntries          = require('./Commands/AclEntries.js'),
      Commands_AuthorizedApps      = require('./Commands/AuthorizedApps.js'),
      Commands_Commands            = require('./Commands/Commands.js'),
      Commands_Devices             = require('./Commands/Devices.js'),
      Commands_Events              = require('./Commands/Events.js'),
      Commands_ModelManifests      = require('./Commands/ModelManifests.js'),
      Commands_PersonalizedInfos   = require('./Commands/PersonalizedInfos.js'),
      Commands_Places              = require('./Commands/Places.js'),
      Commands_RegistrationTickets = require('./Commands/RegistrationTickets.js'),
      Commands_Rooms               = require('./Commands/Rooms.js'),
      Commands_Subscriptions       = require('./Commands/Subscriptions.js');

const SERVER = require('restify')
                    .createServer({
                        name: 'Weaviate Server'
                    });

module.exports = (i) => {
    /**
     * Check if all mandatory fields are set
     */
    if (i === undefined) {
        console.error('Values aren\'t set when you call Weaviate, please pass an object with proper values. More info on the website');
    }
    if (i.hostname === undefined) {
        console.error('Hostname not set, default to localhost');
        i.hostname = 'localhost';
    }
    if (i.port === undefined) {
        console.error('Hostname not set, default to 9000');
        i.port = 9000;
    }
    if (i.formatIn === undefined) {
        console.error('Format not set, default to JSON');
        i.formatIn = 'JSON';
    }
    if (i.dbHostname === undefined) {
        console.error('DB hostname not set, default to localhost');
        i.dbHostname = 'localhost';
    }
    if (i.dbPort === undefined) {
        console.error('DB port not set, default to 9160');
        i.dbPort = 9160;
    }
    if (i.dbContactpoints === undefined) {
        console.error('No Cassandra contactPoints set, default to 127.0.0.1');
        i.dbContactpoints = ['127.0.0.1'];
    }
    if (i.dbKeyspace === undefined) {
        console.error('You need to set a keyspace name (dbKeyspace) for Cassandra');
    }
    if (i.formatIn === undefined) {
        console.error('Format is not set, default to JSON');
        i.formatIn = 'JSON';
    }
    if (i.formatOut === undefined) {
        console.error('Format is not set, default to JSON');
        i.formatOut = 'JSON';
    }
    if (i.stdoutLog === undefined) {
        console.error('stdout_log is not set, default to false');
        i.stdoutLog = false;
    }
    if (i.https === undefined) {
        console.error('https is not set, default to false');
        i.https = false;
    }
    if (i.httpsOpts === undefined && i.https === true) {
        console.error('You want to use HTTPS, make sure to add https_opts');
    }
    if (i.dbName === undefined) {
        console.error('Set a db_name value');
    }
    if (i.dbPassword === undefined) {
        console.error('Set a db_password value');
    }

    /*****************************
     * START LISTENING TO REQUESTS
     */

    /*****************************
     * aclEntries COMMANDS
     */

    /**
     * Id: weave.aclEntries.delete
     * Deletes an ACL entry.
     */
    SERVER.del('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        return new Commands_AclEntries(req, res, next)
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
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.delete');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.delete');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.aclEntries.get
     * Returns the requested ACL entry.
     */
    SERVER.get('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        return new Commands_AclEntries(req, res, next)
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
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.get');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.get');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.aclEntries.insert
     * Inserts a new ACL entry.
     */
    SERVER.post('/devices/:deviceId/aclEntries', (req, res, next) => {
        return new Commands_AclEntries(req, res, next)
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
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.insert');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.insert');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.aclEntries.list
     * Lists ACL entries.
     */
    SERVER.get('/devices/:deviceId/aclEntries', (req, res, next) => {
        return new Commands_AclEntries(req, res, next)
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
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.list');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.list');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.aclEntries.patch
     * Update an ACL entry. This method supports patch semantics.
     */
    SERVER.patch('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        return new Commands_AclEntries(req, res, next)
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
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.patch');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.patch');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.aclEntries.update
     * Update an ACL entry.
     */
    SERVER.put('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        return new Commands_AclEntries(req, res, next)
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
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.aclEntries.update');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.update');
                    }
                    return next();
                })
    });

    /*****************************
     * AuthorizedApps COMMANDS
     */

    /**
     * Id: weave.authorizedApps.createAppAuthenticationToken
     * Generate a token used to authenticate an authorized app.
     */
    SERVER.post('/authorizedApps/createAppAuthenticationToken', (req, res, next) => {
        return new Commands_AuthorizedApps(req, res, next)
                .getCreateAppAuthenticationToken({
                    requiredParams: [],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.authorizedApps.createAppAuthenticationToken');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.authorizedApps.createAppAuthenticationToken');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.authorizedApps.list
     * The actual list of authorized apps.
     */
    SERVER.get('/authorizedApps', (req, res, next) => {
        return new Commands_AuthorizedApps(req, res, next)
                .getList({
                    requiredParams: [],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.authorizedApps.list');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.authorizedApps.list');
                    }
                    return next();
                })
    });

    /*****************************
     * Commands COMMANDS
     */

    /**
     * Id: weave.commands.cancel
     * Cancels a command.
     */
    SERVER.post('/commands/:commandId/cancel', (req, res, next) => {
        return new Commands_Commands(req, res, next)
                .getCancel({
                    requiredParams: [],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.cancel');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.cancel');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.commands.delete
     * Deletes a command.
     */
    SERVER.del('/commands/:commandId', (req, res, next) => {
        return new Commands_Commands(req, res, next)
                .getDelete({
                    requiredParams: [{
                        name: 'commandId',
                        location: 'object'
                    }],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.delete');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.delete');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.commands.get
     * Returns a particular command.
     */
    SERVER.get('/commands/:commandId', (req, res, next) => {
        return new Commands_Commands(req, res, next)
                .getGet({
                    requiredParams: [{
                        name: 'commandId',
                        location: 'object'
                    }],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.get');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.get');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.commands.getQueue
     * Returns queued commands that device is supposed to execute. This method may be used only by devices.
     */
    SERVER.get('/commands/queue', (req, res, next) => {
        return new Commands_Commands(req, res, next)
                .getQueue({
                    requiredParams: ['commandId'],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.getQueue');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.getQueue');
                    }
                    return next();
                })
    });

    /**
     * Id: weave.commands.insert
     * Creates and sends a new command.
     */
    SERVER.post('/commands', (req, res, next) => {
        return new Commands_Commands(req, res, next)
                .getInsert({
                    requiredParams: [],
                    requestObjectName: 'commands',
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.insert');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.insert');
                    }
                    return next();
                })

        /**
         * Id: weave.commands.list
         * Lists all commands in reverse order of creation.
         */
        SERVER.get('/commands', (req, res, next) => {
            return new Commands_Commands(req, res, next)
                    .getList({
                        requiredParams: [{
                            name: 'deviceId',
                            location: 'object'                            
                        }],
                        requestObjectName: 'commands',
                        authScopes: ['/auth/weave.app']
                    })
                    .then(result => {
                        // send the result
                        res.send(result);
                        // exec the onSuccess
                        if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                            i.onSuccess({
                                params:         req.params,
                                body:           req.body,
                                response:       result,
                                requestHeaders: req.headers
                            })
                        }
                        // exec the debug
                        if(i.debug === true){
                            console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.list');
                        }
                        return next();
                    })
                    .catch(error => {
                        res.send(
                            new Helpers_ErrorHandling.createErrorMessage(error)
                        );
                        // exec the onError
                        if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                            i.onError({
                                params:         req.params,
                                body:           req.body,
                                requestHeaders: req.headers
                            })
                        }
                        // exec the debug
                        if(i.debug === true){
                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.list');
                        }
                        return next();
                    })

    });

    /**
     * Id: weave.commands.patch
     * Updates a command. This method may be used only by devices. This method supports patch semantics.
     */
    SERVER.patch('/commands/:commandId', (req, res, next) => {
        return new Commands_Commands(req, res, next)
                .getPatch({
                    requiredParams: [{
                        name: 'commandId',
                        location: 'path'                            
                    }],
                    requestObjectName: 'commands',
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    // send the result
                    res.send(result);
                    // exec the onSuccess
                    if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                        i.onSuccess({
                            params:         req.params,
                            body:           req.body,
                            response:       result,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'SUCCESS', 'weave.commands.patch');
                    }
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    // exec the onError
                    if(i.onError !== undefined && typeof i.onSuccess === 'function'){
                        i.onError({
                            params:         req.params,
                            body:           req.body,
                            requestHeaders: req.headers
                        })
                    }
                    // exec the debug
                    if(i.debug === true){
                        console.log(req.connection.remoteAddress, 'ERROR', 'weave.commands.patch');
                    }
                    return next();
                })

    });

    /*
     * START THE SERVER
     */
    SERVER.listen(i.port);
    console.log('WEAVIATE IS LISTENING ON PORT: ' + i.port);
};
