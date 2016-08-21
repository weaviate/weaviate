'use strict';
/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

const Helpers_ErrorHandling = require('../Helpers/ErrorHandling.js');

/**
 * CLASS Helpers_RESTRender
 */
module.exports = class Helpers_RESTRender {

    /**
     * Constructor for this class
     * @param  {object}  discoveryDocument - the Discovery Document
     */
    constructor(discoveryDocument){
        this.discovery = discoveryDocument,
        this.commands  = {};
    }

    /**
     * Loads the commands from the /Command dir and checks if all commands exist
     * @param  {object}  dbAdapter - the choosen DB
     * @param  {boolean} debug     - debug function true/false, if true, show all console.logs
     * @return {object}  this
     */
    loadCommands(dbAdapter, debug){
        for (var commandKey in this.discovery.resources) { // loop through main command throughts
            if(typeof commandKey === 'string'){
                /**
                 * Create directory names and command groups
                 */
                let commandGroup    = commandKey.charAt(0).toUpperCase() + commandKey.slice(1), // create directory name
                    commandLocation = './Commands/' + dbAdapter + '/' + commandGroup + '.js';

                /**
                 * Check if the command file exists, if so, add it.
                 */
                require('fs').existsSync(commandLocation); // check if the file exist
                if(debug === true){
                    console.log('✓ ' + commandLocation); // Command file is found
                }
                this.commands[commandGroup] = require('.' + commandLocation); // note the first dot, it makes the commandLocation ../
            }
        }
        return this;
    }

    /**
     * Creates HTTPS REST API
     * @param  {object} i - check if HTTPS is set, if not, don't create HTTPS server and return
     * @return {object} this
     */
    createHttps(i){
        if(i.https === false){
            return this; // return directly, no HTTPS is set
        } else {
            /**
             * Load Restify
             */
            var RESTIFY = require('restify'),
                SERVER  = RESTIFY
                            .createServer({
                                name: 'Weaviate Server'
                            })
                            .pre(RESTIFY.pre.sanitizePath());

            /**
             * Set commands
             */
            var commands = this.commands;

            /**
             * Create commandsObject which will function as a map to all functions
             */
            var commandsObject = {};

            /**
             * Generate all endpoints by looping through discovery document
             */
            for (var commandKey in this.discovery.resources) {

                if(typeof commandKey === 'string'){

                    var commandGroup = commandKey.charAt(0).toUpperCase() + commandKey.slice(1); // create group name

                    for (var methodKey in this.discovery.resources[commandKey].methods) {

                        if(typeof methodKey === 'string'){

                            let actionObject = this.discovery.resources[commandKey].methods[methodKey];
                            let path = '/' + actionObject.path.replace(/{/g, ':').replace(/}/g, '');

                            /**
                             * Set correct Restify function name
                             *
                             */
                            var restifyFunctionName;
                            switch(actionObject.httpMethod) {
                                case 'GET':
                                    restifyFunctionName = 'get';
                                    break;
                                case 'POST':
                                    restifyFunctionName = 'post';
                                    break;
                                case 'DELETE':
                                    restifyFunctionName = 'del';
                                    break;
                                case 'PATCH':
                                    restifyFunctionName = 'patch';
                                    break;
                                case 'PUT':
                                    restifyFunctionName = 'put';
                                    break;
                                default:
                                    throw new Error('WEAVIATE ERROR, THIS REST METHOD IS NOT FOUND: ' + actionObject.httpMethod);
                            }

                            /**
                             * Add actionObject and commandGroup to the commandsObjects
                             */
                            commandsObject[actionObject.httpMethod + '_' + path] = {};
                            commandsObject[actionObject.httpMethod + '_' + path].actionObject = actionObject;
                            commandsObject[actionObject.httpMethod + '_' + path].commandGroup = commandGroup;

                            /**
                             * Add function to the server request
                             */
                            SERVER[restifyFunctionName](path, (req, res, next) => {

                                /**
                                 * GET THE CORRECT actionObject and commandGroup
                                 */
                                var actionObject = commandsObject[req.route.method + '_' + req.route.path].actionObject;
                                var commandGroup = commandsObject[req.route.method + '_' + req.route.path].commandGroup;

                                /**
                                 * Set variables needed to run command
                                 */
                                var subCommand = actionObject.id.split('.');

                                return new commands[commandGroup](req, res, next)
                                    ['$' + subCommand[subCommand.length - 1].charAt(0).toUpperCase() + subCommand[subCommand.length - 1].slice(1)]({
                                        requiredParams: actionObject.parameters,
                                        requestObject: this.discovery.schemas[actionObject.response.$ref], // $ref to actual object
                                        responseObject: this.discovery.schemas[actionObject.response.$ref], // $ref to actual object
                                        authScopes: actionObject.scopes
                                    })
                                    .then(result => {
                                        // send the result
                                        res.send(result);
                                        // exec the onSuccess
                                        if(i.onSuccess !== undefined && typeof i.onSuccess === 'function'){
                                            i.onSuccess({
                                                params: req.params,
                                                body: req.body,
                                                response: result,
                                                requestHeaders: req.headers
                                            });
                                        }
                                        // exec the debug
                                        if(i.debug === true){
                                            console.log(req.connection.remoteAddress, 'SUCCESS', actionObject.id);
                                        }
                                        return next();
                                    })
                                    .catch(error => {
                                        res.send(
                                            new Helpers_ErrorHandling.createErrorMessage(error)
                                        );
                                        // exec the onError
                                        if(i.onError !== undefined && typeof i.onError === 'function'){
                                            i.onError({
                                                params: req.params,
                                                body: req.body,
                                                requestHeaders: req.headers
                                            });
                                        }
                                        // exec the debug
                                        if(i.debug === true){
                                            console.log(req.connection.remoteAddress, 'ERROR', actionObject.id);
                                        }
                                        return next();
                                    });
                            });

                            /**
                             * Return that the function is created
                             */
                            if(i.debug === true){
                                console.log('✓ HTTPS REST action ' + actionObject.id + ' (' + path + ') created');
                            }
                        }
                    }
                }
            }

            /**
             * Start listening
             */
            SERVER.listen(i.https.port);
            console.log('✓ WEAVIATE HTTPS REST API IS LISTENING ON PORT: ' + i.https.port);
            return this;
        }
    }

    /**
     * Creates MQTT REST API
     * @param  {object} i - check if MQTT is set, if not, don't create MQTT server and return
     * @return {object} this
     */
    createMqtt(i){
        if(i.mqtt === false){
            return this; // return directly, no MQTT is set
        } else {
            console.log('CREATE MQTT');
            return this;
        }
    }

};
