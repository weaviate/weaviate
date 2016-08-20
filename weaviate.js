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
module.exports = (weaviate) => {

    /*****************************
     * Check and validate if all mandatory fields are set
     */
    if (weaviate === undefined) {
        console.error('Values aren\'t set when you call Weaviate, please pass an object with proper values. More info on the website');
    } else {
        weaviate = weaviate; // make i available globally
    }
    if (weaviate.hostname === undefined) {
        console.error('Hostname not set, default to localhost');
        weaviate.hostname = 'localhost';
    }
    if (weaviate.https.port === undefined) {
        console.error('Hostname not set, default to 9000');
        weaviate.port = 9000;
    }
    if (weaviate.formatIn === undefined) {
        console.error('Format not set, default to JSON');
        weaviate.formatIn = 'JSON';
    }
    if (weaviate.db === undefined) {
        console.error('Set the Database object');
        weaviate.dbHostname = 'localhost';
    }
    if (weaviate.db.dbAdapter === undefined) {
        console.error('Set a dbAdapter value');
        throw new Error();
    }
    if (weaviate.formatIn === undefined) {
        console.error('Format is not set, default to JSON');
        weaviate.formatIn = 'JSON';
    }
    if (weaviate.formatOut === undefined) {
        console.error('Format is not set, default to JSON');
        weaviate.formatOut = 'JSON';
    }
    if (weaviate.stdoutLog === undefined) {
        console.error('stdout_log is not set, default to false');
        weaviate.stdoutLog = false;
    }
    if (weaviate.mqtt === undefined) {
        console.warn('mqtt is not set, default to false, no mqtt REST API');
        weaviate.mqtt = false;
    }
    if (weaviate.https === undefined) {
        console.warn('https is not set, default to false, no https REST API');
        weaviate.https = false;
    }
    if (weaviate.weaveDiscovery === undefined) {
        console.warn('No discovery document set, default to latest Weave Discovery Document');
        weaviate.weaveDiscovery = './weave.json';
    }

    /*****************************
     * Add the Classes
     */
    const Commands_AclEntries           = require('./Commands/' + weaviate.db.dbAdapter + '/AclEntries.js'),
          Commands_Adapters             = require('./Commands/' + weaviate.db.dbAdapter + '/Adapters.js'),
          Commands_AuthorizedApps       = require('./Commands/' + weaviate.db.dbAdapter + '/AuthorizedApps.js'),
          Commands_Commands             = require('./Commands/' + weaviate.db.dbAdapter + '/Commands.js'),
          Commands_Devices              = require('./Commands/' + weaviate.db.dbAdapter + '/Devices.js'),
          Commands_Events               = require('./Commands/' + weaviate.db.dbAdapter + '/Events.js'),
          Commands_ModelManifests       = require('./Commands/' + weaviate.db.dbAdapter + '/ModelManifests.js'),
          Commands_PersonalizedInfos    = require('./Commands/' + weaviate.db.dbAdapter + '/PersonalizedInfos.js'),
          Commands_Places               = require('./Commands/' + weaviate.db.dbAdapter + '/Places.js'),
          Commands_RegistrationTickets  = require('./Commands/' + weaviate.db.dbAdapter + '/RegistrationTickets.js'),
          Commands_Rooms                = require('./Commands/' + weaviate.db.dbAdapter + '/Rooms.js'),
          Commands_Subscriptions        = require('./Commands/' + weaviate.db.dbAdapter + '/Subscriptions.js'),
          Helpers_RESTRender            = require('./Helpers/RESTRender.js'),
          Helpers_ErrorHandling         = require('./Helpers/ErrorHandling.js');

    /*****************************
     * Create the REST API for HTTPS and/or MQTT
     */
    new Helpers_RESTRender(require(weaviate.weaveDiscovery))
            .loadCommands(weaviate.db.dbAdapter, weaviate.debug) // Loads command classes from the /Commands dir and validates them
            .createHttps(weaviate) // Creates the Https REST API if weaviate.https !== false
            .createMqtt(weaviate); // Creates the Mqtt REST API if weaviate.mqtt !== false


};
