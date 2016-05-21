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

const Commands_AclEntries = require('./Commands/AclEntries.js');

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
        new Commands_AclEntries(req, res, next)
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
                    res.send(result);
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    next();
                });
    });

    /**
     * Id: weave.aclEntries.get
     * Returns the requested ACL entry.
     */
    SERVER.get('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        new Commands_AclEntries(req, res, next)
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
                    res.send(result);
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    next();
                });
    });

    /**
     * Id: weave.aclEntries.insert
     * Inserts a new ACL entry.
     */
    SERVER.post('/devices/:deviceId/aclEntries', (req, res, next) => {
        new Commands_AclEntries(req, res, next)
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
                    res.send(result);
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    next();
                });
    });

    /**
     * Id: weave.aclEntries.list
     * Lists ACL entries.
     */
    SERVER.get('/devices/:deviceId/aclEntries', (req, res, next) => {
        new Commands_AclEntries(req, res, next)
                .getList({
                    requiredParams: [{
                        name: 'deviceId',
                        location: 'path'
                    }],
                    requestObjectName: null,
                    authScopes: ['/auth/weave.app']
                })
                .then(result => {
                    res.send(result);
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    next();
                });
    });

    /**
     * Id: weave.aclEntries.patch
     * Update an ACL entry. This method supports patch semantics.
     */
    SERVER.patch('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        new Commands_AclEntries(req, res, next)
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
                    res.send(result);
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    next();
                });
    });

    /**
     * Id: weave.aclEntries.update
     * Update an ACL entry.
     */
    SERVER.put('/devices/:deviceId/aclEntries/:aclEntryId', (req, res, next) => {
        new Commands_AclEntries(req, res, next)
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
                    res.send(result);
                    return next();
                })
                .catch(error => {
                    res.send(
                        new Helpers_ErrorHandling.createErrorMessage(error)
                    );
                    next();
                });
    });

    /*
     * START THE SERVER
     */
    SERVER.listen(i.port);
    console.log('WEAVIATE IS LISTENING ON PORT: ' + i.port);
};
