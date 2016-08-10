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

/*****************************
 * Export the Weaviate module
 * @param  {object} i  - Object that contains all info to start a Weaviate server
 * @return {export} Returns the Weaviate Object
 */
module.exports = (i) => {

    /*****************************
     * Check and validate if all mandatory fields are set
     */
    if (i === undefined) {
        console.error('Values aren\'t set when you call Weaviate, please pass an object with proper values. More info on the website');
    } else {
        global.i = i; // make i available globally
    }
    if (global.i.hostname === undefined) {
        console.error('Hostname not set, default to localhost');
        global.i.hostname = 'localhost';
    }
    if (global.i.port === undefined) {
        console.error('Hostname not set, default to 9000');
        global.i.port = 9000;
    }
    if (global.i.formatIn === undefined) {
        console.error('Format not set, default to JSON');
        global.i.formatIn = 'JSON';
    }
    if (global.i.db === undefined) {
        console.error('Set the Database object');
        global.i.dbHostname = 'localhost';
    }
    if (global.i.db.dbAdapter === undefined) {
        console.error('Set a dbAdapter value');
    }
    if (global.i.formatIn === undefined) {
        console.error('Format is not set, default to JSON');
        global.i.formatIn = 'JSON';
    }
    if (global.i.formatOut === undefined) {
        console.error('Format is not set, default to JSON');
        global.i.formatOut = 'JSON';
    }
    if (global.i.stdoutLog === undefined) {
        console.error('stdout_log is not set, default to false');
        global.i.stdoutLog = false;
    }
    if (global.i.https === undefined) {
        console.error('https is not set, default to false');
        global.i.https = false;
    }
    if (global.i.httpsOpts === undefined && i.https === true) {
        console.error('You want to use HTTPS, make sure to add https_opts');
    }

    /*****************************
     * Add the Classes
     */
    const Commands_AclEntries           = require('./Commands/' + global.i.db.dbAdapter + '/AclEntries.js'),
          Commands_Adapters             = require('./Commands/' + global.i.db.dbAdapter + '/Adapters.js'),
          Commands_AuthorizedApps       = require('./Commands/' + global.i.db.dbAdapter + '/AuthorizedApps.js'),
          Commands_Commands             = require('./Commands/' + global.i.db.dbAdapter + '/Commands.js'),
          Commands_Devices              = require('./Commands/' + global.i.db.dbAdapter + '/Devices.js'),
          Commands_Events               = require('./Commands/' + global.i.db.dbAdapter + '/Events.js'),
          Commands_ModelManifests       = require('./Commands/' + global.i.db.dbAdapter + '/ModelManifests.js'),
          Commands_PersonalizedInfos    = require('./Commands/' + global.i.db.dbAdapter + '/PersonalizedInfos.js'),
          Commands_Places               = require('./Commands/' + global.i.db.dbAdapter + '/Places.js'),
          Commands_RegistrationTickets  = require('./Commands/' + global.i.db.dbAdapter + '/RegistrationTickets.js'),
          Commands_Rooms                = require('./Commands/' + global.i.db.dbAdapter + '/Rooms.js'),
          Commands_Subscriptions        = require('./Commands/' + global.i.db.dbAdapter + '/Subscriptions.js'),
          Endpoints_AclEntries          = require('./Endpoints/AclEntries.js'),
          Endpoints_Adapters            = require('./Endpoints/Adapters.js'),
          Endpoints_AuthorizedApps      = require('./Endpoints/AuthorizedApps.js'),
          Endpoints_Commands            = require('./Endpoints/Commands.js'),
          Endpoints_Devices             = require('./Endpoints/Devices.js'),
          Endpoints_Events              = require('./Endpoints/Events.js'),
          Endpoints_ModelManifests      = require('./Endpoints/ModelManifests.js'),
          Endpoints_PersonalizedInfos   = require('./Endpoints/PersonalizedInfos.js'),
          Endpoints_Places              = require('./Endpoints/Places.js'),
          Endpoints_RegistrationTickets = require('./Endpoints/RegistrationTickets.js'),
          Endpoints_Rooms               = require('./Endpoints/Rooms.js'),
          Endpoints_Subscriptions       = require('./Endpoints/Subscriptions.js'),
          Helpers_ErrorHandling         = require('./Helpers/ErrorHandling.js');

    /*****************************
     * If MQTT is set, start MQTT server
     */
    if(global.i.mqtt !== undefined){
        
        /**
         * Include Mosca
         */
        const Mqtt_Mosca = require('mosca');

        /**
         * Set Redis settings
         */
        global.i.mqtt['persistence']            = { 'factory': Mqtt_Mosca.persistence.Redis };
        global.i.mqtt.backend['type']           = 'redis';
        global.i.mqtt.backend['redis']          = require('redis');
        global.i.mqtt.backend['return_buffers'] = true;
        global.i.mqtt.backend['db']             = 12;

        /**
         * Load MQTT-server
         */
        var Mqtt_Server = new Mqtt_Mosca.Server(global.i.mqtt.settings);

        /**
         * On mqtt client connect
         */
        Mqtt_Server.on('clientConnected', function(client) {
            if(global.i.debug === true){
                console.log('WEAVIATE-MQTT', client.id, 'CONNECTED');
            }
        });

        /**
         * On mqtt publish
         */
        Mqtt_Server.on('published', function(packet, client) {
            if(global.i.debug === true){
                console.log('WEAVIATE-MQTT', 'PAYLOAD', 'SUCCESS');
            }
        });

        /**
         * Start MQTT-server
         */
        Mqtt_Server.on('ready', () => {
            console.log('WEAVIATE-MQTT PUB-SUB IS LISTENING ON PORT:', global.i.mqtt.port);
        });

    }

    /*****************************
     * Add the server consts
     */
    const RESTIFY = require('restify'),
          SERVER  = RESTIFY
                        .createServer({
                            name: 'Weaviate Server'
                        })
                        .pre(RESTIFY.pre.sanitizePath());

    /*****************************
     * START LISTENING TO ENDPOINT REQUESTS
     */

    /*****************************
     * aclEntries Endpoints -> Commands
     */
    new Endpoints_AclEntries(SERVER, Commands_AclEntries);

    /*****************************
     * AuthorizedApps Endpoints -> Commands
     */
    new Endpoints_Adapters(SERVER, Commands_AuthorizedApps);

    /*****************************
     * AuthorizedApps Endpoints -> Commands
     */
    new Endpoints_AuthorizedApps(SERVER, Commands_AuthorizedApps);

    /*****************************
     * Commands Endpoints -> Commands
     */
    new Endpoints_Commands(SERVER, Commands_Commands);

    /*****************************
     * Devices Endpoints -> Commands
     */
    new Endpoints_Devices(SERVER, Commands_Devices);

    /*****************************
     * Events Endpoints -> Commands
     */
    new Endpoints_Events(SERVER, Commands_Events);

    /*****************************
     * ModelManifests Endpoints -> Commands
     */
    new Endpoints_ModelManifests(SERVER, Commands_ModelManifests);

    /*****************************
     * PersonalizedInfos Endpoints -> Commands
     */
    new Endpoints_PersonalizedInfos(SERVER, Commands_PersonalizedInfos);

    /*****************************
     * Places Endpoints -> Commands
     */
    new Endpoints_Places(SERVER, Commands_Places);

    /*****************************
     * RegistrationTickets Endpoints -> Commands
     */
    new Endpoints_RegistrationTickets(SERVER, Commands_RegistrationTickets);

    /*****************************
     * Rooms Endpoints -> Commands
     */
    new Endpoints_Rooms(SERVER, Commands_Rooms);

    /*****************************
     * Subscriptions Endpoints -> Commands
     */
    new Endpoints_Subscriptions(SERVER, Commands_Subscriptions);

    /*****************************
     * DONE LISTENING TO ENDPOINT REQUESTS
     */

    /*****************************
     * START THE SERVER
     */
    SERVER.listen(global.i.port);
    console.log('WEAVIATE IS LISTENING ON PORT: ' + global.i.port);
};
