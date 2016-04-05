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
module.exports = (i) => {
    /**
     * Check if all input fields are set
     */
    if (i === undefined) {
        throw "Values aren't set when you call Weaviate, please pass an object with proper values. More info on the website";
    } else if (i.hostname === undefined) {
        console.warn("Hostname not set, default to localhost");
        i.hostname = 'localhost';
    } else if (i.port === undefined) {
        console.warn("Hostname not set, default to 9000");
        i.port = 9000;
    } else if (i.format === undefined) {
        console.warn("Format not set, default to JSON");
        i.format = 'JSON';
    } else if (i.db_hostname === undefined) {
        console.warn("DB hostname not set, default to localhost");
        i.db_hostname = 'localhost';
    } else if (i.db_port === undefined) {
        console.warn("DB port not set, default to 9160");
        i.db_port = 9160;
    } else if (i.format_in === undefined) {
        console.warn("Format is not set, default to JSON");
        i.format_in = 'JSON';
    } else if (i.format_out === undefined) {
        console.warn("Format is not set, default to JSON");
        i.format_out = 'JSON';
    } else if (i.stdout_log === undefined) {
        console.warn("stdout_log is not set, default to false");
        i.stdout_log = false;
    } else if (i.https === undefined) {
        console.warn("https is not set, default to false");
        i.https = false;
    } else if (i.https_opts === undefined && i.https === true) {
        throw "You want to use HTTPS, make sure to add https_opts";
    } else if (i.db_name === undefined) {
        throw "Set a db_name value";
    } else if (i.db_password === undefined) {
        throw "Set a db_password value";
    }
    /**
     * Include all deps and set all vars
     */
    const HTTP = require('http'),
        HTTPS = require('https'),
        ACLENTRIES = require('./libs/weave/v1/aclEntries.js'),
        AUTHORIZEDAPPS = require('./libs/weave/v1/authorizedApps.js'),
        COMMANDS = require('./libs/weave/v1/commands.js'),
        DEVICES = require('./libs/weave/v1/devices.js'),
        EVENTS = require('./libs/weave/v1/events.js'),
        MODELMANIFESTS = require('./libs/weave/v1/modelManifests.js'),
        PERSONALIZEDINFOS = require('./libs/weave/v1/personalizedInfos.js'),
        REGISTRATIONTICKETS = require('./libs/weave/v1/registrationTickets.js'),
        SUBSCRIPTIONS = require('./libs/weave/v1/subscriptions.js'),
        HOSTNAME = i.hostname,
        EXPRESS = require('express'),
        Q = require('q'),
        APP = EXPRESS(),
        PORT = i.port;
    var MAINDEFERRED = Q.defer();
    /**
     * Set response fordevices/{deviceId}/aclEntries/{aclEntryId}
     */
    APP.all('/devices/:deviceId/aclEntries/:aclEntryId', (req, res) => {
        /**
         * Deletes an ACL entry.
         */
        if (req.method === 'DELETE') {
            ACLENTRIES
                .delete(req.originalUrl.split('/'), {
                    'aclEntryId': req.params.aclEntryId,
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Returns the requested ACL entry.
         */
        if (req.method === 'GET') {
            ACLENTRIES
                .get(req.originalUrl.split('/'), {
                    'aclEntryId': req.params.aclEntryId,
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Update an ACL entry. This method supports patch semantics.
         */
        if (req.method === 'PATCH') {
            ACLENTRIES
                .patch(req.originalUrl.split('/'), {
                    'aclEntryId': req.params.aclEntryId,
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Update an ACL entry.
         */
        if (req.method === 'PUT') {
            ACLENTRIES
                .update(req.originalUrl.split('/'), {
                    'aclEntryId': req.params.aclEntryId,
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices/{deviceId}/aclEntries
     */
    APP.all('/devices/:deviceId/aclEntries', (req, res) => {
        /**
         * Inserts a new ACL entry.
         */
        if (req.method === 'POST') {
            ACLENTRIES
                .insert(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Lists ACL entries.
         */
        if (req.method === 'GET') {
            ACLENTRIES
                .list(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'maxResults': req.params.maxResults,
                    'startIndex': req.params.startIndex,
                    'token': req.params.token,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forauthorizedApps/createappauthenticationtoken
     */
    APP.all('/authorizedApps/createappauthenticationtoken', (req, res) => {
        /**
         * Generate a token used to authenticate an authorized app.
         */
        if (req.method === 'POST') {
            AUTHORIZEDAPPS
                .createappauthenticationtoken(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forauthorizedApps
     */
    APP.all('/authorizedApps', (req, res) => {
        /**
         * The actual list of authorized apps.
         */
        if (req.method === 'GET') {
            AUTHORIZEDAPPS
                .list(req.originalUrl.split('/'), {
                    'certificateHash': req.params.certificateHash,
                    'hl': req.params.hl,
                    'packageName': req.params.packageName,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forcommands/{commandId}/cancel
     */
    APP.all('/commands/:commandId/cancel', (req, res) => {
        /**
         * Cancels a command.
         */
        if (req.method === 'POST') {
            COMMANDS
                .cancel(req.originalUrl.split('/'), {
                    'commandId': req.params.commandId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forcommands/{commandId}
     */
    APP.all('/commands/:commandId', (req, res) => {
        /**
         * Deletes a command.
         */
        if (req.method === 'DELETE') {
            COMMANDS
                .delete(req.originalUrl.split('/'), {
                    'commandId': req.params.commandId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Returns a particular command.
         */
        if (req.method === 'GET') {
            COMMANDS
                .get(req.originalUrl.split('/'), {
                    'attachmentPath': req.params.attachmentPath,
                    'commandId': req.params.commandId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Updates a command. This method may be used only by devices. This method supports patch semantics.
         */
        if (req.method === 'PATCH') {
            COMMANDS
                .patch(req.originalUrl.split('/'), {
                    'commandId': req.params.commandId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Updates a command. This method may be used only by devices.
         */
        if (req.method === 'PUT') {
            COMMANDS
                .update(req.originalUrl.split('/'), {
                    'commandId': req.params.commandId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forcommands/queue
     */
    APP.all('/commands/queue', (req, res) => {
        /**
         * Returns queued commands that device is supposed to execute. This method may be used only by devices.
         */
        if (req.method === 'GET') {
            COMMANDS
                .getQueue(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forcommands
     */
    APP.all('/commands', (req, res) => {
        /**
         * Creates and sends a new command.
         */
        if (req.method === 'POST') {
            COMMANDS
                .insert(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'responseAwaitMs': req.params.responseAwaitMs,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Lists all commands in reverse order of creation.
         */
        if (req.method === 'GET') {
            COMMANDS
                .list(req.originalUrl.split('/'), {
                    'byUser': req.params.byUser,
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'maxResults': req.params.maxResults,
                    'startIndex': req.params.startIndex,
                    'state': req.params.state,
                    'token': req.params.token,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices/{deviceId}
     */
    APP.all('/devices/:deviceId', (req, res) => {
        /**
         * Deletes a device from the system.
         */
        if (req.method === 'DELETE') {
            DEVICES
                .delete(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Returns a particular device data.
         */
        if (req.method === 'GET') {
            DEVICES
                .get(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Updates a device data. This method supports patch semantics.
         */
        if (req.method === 'PATCH') {
            DEVICES
                .patch(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'lastUpdateTimeMs': req.params.lastUpdateTimeMs,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Updates a device data.
         */
        if (req.method === 'PUT') {
            DEVICES
                .update(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'lastUpdateTimeMs': req.params.lastUpdateTimeMs,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices/{deviceId}/handleInvitation
     */
    APP.all('/devices/:deviceId/handleInvitation', (req, res) => {
        /**
         * Confirms or rejects a pending device.
         */
        if (req.method === 'POST') {
            DEVICES
                .handleInvitation(req.originalUrl.split('/'), {
                    'action': req.params.action,
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'scopeId': req.params.scopeId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices
     */
    APP.all('/devices', (req, res) => {
        /**
         * Registers a new device. This method may be used only by aggregator devices.
         */
        if (req.method === 'POST') {
            DEVICES
                .insert(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Lists devices user has access to.
         */
        if (req.method === 'GET') {
            DEVICES
                .list(req.originalUrl.split('/'), {
                    'descriptionSubstring': req.params.descriptionSubstring,
                    'deviceKind': req.params.deviceKind,
                    'displayNameSubstring': req.params.displayNameSubstring,
                    'hl': req.params.hl,
                    'maxResults': req.params.maxResults,
                    'nameSubstring': req.params.nameSubstring,
                    'role': req.params.role,
                    'startIndex': req.params.startIndex,
                    'systemNameSubstring': req.params.systemNameSubstring,
                    'token': req.params.token,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices/{deviceId}/patchState
     */
    APP.all('/devices/:deviceId/patchState', (req, res) => {
        /**
         * Applies provided patches to the device state. This method may be used only by devices.
         */
        if (req.method === 'POST') {
            DEVICES
                .patchState(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices/{deviceId}/updateParent
     */
    APP.all('/devices/:deviceId/updateParent', (req, res) => {
        /**
         * Updates parent of the child device. Only managers can use this method.
         */
        if (req.method === 'POST') {
            DEVICES
                .updateParent(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'parentId': req.params.parentId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forevents
     */
    APP.all('/events', (req, res) => {
        /**
         * Lists events.
         */
        if (req.method === 'GET') {
            EVENTS
                .list(req.originalUrl.split('/'), {
                    'commandId': req.params.commandId,
                    'deviceId': req.params.deviceId,
                    'endTimeMs': req.params.endTimeMs,
                    'hl': req.params.hl,
                    'maxResults': req.params.maxResults,
                    'startIndex': req.params.startIndex,
                    'startTimeMs': req.params.startTimeMs,
                    'token': req.params.token,
                    'type': req.params.type,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response formodelManifests/{modelManifestId}
     */
    APP.all('/modelManifests/:modelManifestId', (req, res) => {
        /**
         * Returns a particular model manifest.
         */
        if (req.method === 'GET') {
            MODELMANIFESTS
                .get(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'modelManifestId': req.params.modelManifestId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response formodelManifests
     */
    APP.all('/modelManifests', (req, res) => {
        /**
         * Lists all model manifests.
         */
        if (req.method === 'GET') {
            MODELMANIFESTS
                .list(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'ids': req.params.ids,
                    'maxResults': req.params.maxResults,
                    'startIndex': req.params.startIndex,
                    'token': req.params.token,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response formodelManifests/validateCommandDefs
     */
    APP.all('/modelManifests/validateCommandDefs', (req, res) => {
        /**
         * Validates given command definitions and returns errors.
         */
        if (req.method === 'POST') {
            MODELMANIFESTS
                .validateCommandDefs(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response formodelManifests/validateDeviceState
     */
    APP.all('/modelManifests/validateDeviceState', (req, res) => {
        /**
         * Validates given device state object and returns errors.
         */
        if (req.method === 'POST') {
            MODELMANIFESTS
                .validateDeviceState(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response fordevices/{deviceId}/personalizedInfos/{personalizedInfoId}
     */
    APP.all('/devices/:deviceId/personalizedInfos/:personalizedInfoId', (req, res) => {
        /**
         * Returns the personalized info for device.
         */
        if (req.method === 'GET') {
            PERSONALIZEDINFOS
                .get(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'personalizedInfoId': req.params.personalizedInfoId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Update the personalized info for device. This method supports patch semantics.
         */
        if (req.method === 'PATCH') {
            PERSONALIZEDINFOS
                .patch(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'personalizedInfoId': req.params.personalizedInfoId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Update the personalized info for device.
         */
        if (req.method === 'PUT') {
            PERSONALIZEDINFOS
                .update(req.originalUrl.split('/'), {
                    'deviceId': req.params.deviceId,
                    'hl': req.params.hl,
                    'personalizedInfoId': req.params.personalizedInfoId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forregistrationTickets/{registrationTicketId}/finalize
     */
    APP.all('/registrationTickets/:registrationTicketId/finalize', (req, res) => {
        /**
         * Finalizes device registration and returns its credentials. This method may be used only by devices.
         */
        if (req.method === 'POST') {
            REGISTRATIONTICKETS
                .finalize(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'registrationTicketId': req.params.registrationTicketId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forregistrationTickets/{registrationTicketId}
     */
    APP.all('/registrationTickets/:registrationTicketId', (req, res) => {
        /**
         * Returns an existing registration ticket.
         */
        if (req.method === 'GET') {
            REGISTRATIONTICKETS
                .get(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'registrationTicketId': req.params.registrationTicketId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Updates an existing registration ticket. This method supports patch semantics.
         */
        if (req.method === 'PATCH') {
            REGISTRATIONTICKETS
                .patch(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'registrationTicketId': req.params.registrationTicketId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
        /**
         * Updates an existing registration ticket.
         */
        if (req.method === 'PUT') {
            REGISTRATIONTICKETS
                .update(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'registrationTicketId': req.params.registrationTicketId,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forregistrationTickets
     */
    APP.all('/registrationTickets', (req, res) => {
        /**
         * Creates a new registration ticket.
         */
        if (req.method === 'POST') {
            REGISTRATIONTICKETS
                .insert(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * Set response forsubscriptions/subscribe
     */
    APP.all('/subscriptions/subscribe', (req, res) => {
        /**
         * Subscribes the authenticated user and application to receiving notifications.
         */
        if (req.method === 'POST') {
            SUBSCRIPTIONS
                .subscribe(req.originalUrl.split('/'), {
                    'hl': req.params.hl,
                    'body': req.body
                }, Q)
                .then((callbackObj) => {
                    /**
                     * Send the response back
                     */
                    res.json(callbackObj);
                    /**
                     * Resolve promise and send back the weaveObject
                     */
                    MAINDEFERRED.resolve({
                        params: req.params,
                        body: req.body,
                        response: callbackObj,
                        requestHeaders: req.headers,
                        connection: req.connection
                    });
                })
                .fail((callbackObj) => {
                    res
                        .status(404)
                        .json({
                            "ERROR": callbackObj
                        });
                })
        }
    });
    /**
     * If nothing is found...
     */
    APP.use(PORT, (req, res) => {
        res.json({
            'error': 'not found'
        })
    });
    /**
     * Launch the APP
     */
    if (i.https === true) {
        HTTPS.createServer(i.https_opts, APP)
            .listen(PORT, () => {
                console.log('Weaviate is listening via HTTPS on port ' + PORT);
            });
    } else {
        HTTP.createServer(APP)
            .listen(PORT, () => {
                console.log('Weaviate is listening via HTTP on port ' + PORT);
            });
    }
    return MAINDEFERRED.promise;
}