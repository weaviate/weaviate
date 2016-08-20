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

/**
 * CLASS Helpers_RESTRender
 */
module.exports = class Helpers_RESTRender {

	constructor(discoveryDocument){
		this.discovery 	= discoveryDocument,
		this.commands 	= {};
	}

	/**
	 * Loads the commands from the /Command dir and checks if all commands exist
	 */
	loadCommands(dbAdapter, debug){
		for (var commandKey in this.discovery.resources) { // loop through main command throughts
			/**
			 * Create directory names and command groups
			 */
			let commandGroup 	= commandKey.charAt(0).toUpperCase() + commandKey.slice(1), // create directory name
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
		return this;
	}

	/**
	 * Creates HTTPS REST API
	 * i = check if HTTPS is set, if not, don't create HTTPS server and return
	 */
	createHttps(i){
		if(i.https === false){
			return this; // return directly, no HTTPS is set
		} else {
			/**
			 * Load Restify
			 */
			var 	RESTIFY = require('restify'),
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
             * Generate all endpoints by looping through discovery document
             */
            for (var commandKey in this.discovery.resources) {

            	var commandGroup = commandKey.charAt(0).toUpperCase() + commandKey.slice(1); // create group name
            	
            	for (var methodKey in this.discovery.resources[commandKey].methods) {
            		let actionObject = this.discovery.resources[commandKey].methods[methodKey];
            		let path = '/' + actionObject.path.replace(/{/g, ':').replace(/}/g, '');

            		/**
            		 * Set correct Restify function name
            		 * 
            		 */
            		switch(actionObject.httpMethod) {
					    case 'GET':
							var restifyFunctionName = 'get';
					        break;
					    case 'POST':
					    	var restifyFunctionName = 'post';
					        break;
					    case 'DELETE':
					    	var restifyFunctionName = 'del';
					    	break;
					    case 'PATCH':
					    	var restifyFunctionName = 'patch';
					    	break;
					    case 'PUT':
					    	var restifyFunctionName = 'put';
					    	break;
					    default:
					    	throw new Error('WEAVIATE ERROR, THIS REST METHOD IS NOT FOUND: ' + actionObject.httpMethod);
					    	break;
					}



					/**
					 * Add function to the server request
					 */
					SERVER[restifyFunctionName](path, (req, res, next) => {

						/****
						 *
						 * GET THE CORRECT actionObject and commandGroup!
						 *
						 * This can be done be creating an array OUTSIDE server, save in this path + method. This can be retrieved from req.route.path and req.route.method
						 *
						 */
						console.log(req);

						/**
						 * Set variables needed to run command
						 */
						var subCommand = actionObject.id.split('.');

						return new commands[commandGroup](req, res, next)
		                    ['$' + subCommand[subCommand.length-1]]({
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
		                                params: req.params,
		                                body: req.body,
		                                response: result,
		                                requestHeaders: req.headers
		                            });
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
		                        if(i.onError !== undefined && typeof i.onError === 'function'){
		                            i.onError({
		                                params: req.params,
		                                body: req.body,
		                                requestHeaders: req.headers
		                            });
		                        }
		                        // exec the debug
		                        if(i.debug === true){
		                            console.log(req.connection.remoteAddress, 'ERROR', 'weave.aclEntries.delete');
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
	 * i = check if MQTT is set, if not, don't create MQTT server and return
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
