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
 * Add the Classes
 */
const Commands_AclEntries           = require('./Commands/AclEntries.js'),
      Commands_AuthorizedApps       = require('./Commands/AuthorizedApps.js'),
      Commands_Commands             = require('./Commands/Commands.js'),
      Commands_Devices              = require('./Commands/Devices.js'),
      Commands_Events               = require('./Commands/Events.js'),
      Commands_ModelManifests       = require('./Commands/ModelManifests.js'),
      Commands_PersonalizedInfos    = require('./Commands/PersonalizedInfos.js'),
      Commands_Places               = require('./Commands/Places.js'),
      Commands_RegistrationTickets  = require('./Commands/RegistrationTickets.js'),
      Commands_Rooms                = require('./Commands/Rooms.js'),
      Commands_Subscriptions        = require('./Commands/Subscriptions.js'),
      Endpoints_AclEntries          = require('./Endpoints/AclEntries.js'),
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
 * Add the server consts
 */
const RESTIFY = require('restify'),
      SERVER  = RESTIFY
                    .createServer({
                        name: 'Weaviate Server'
                    })
                    .pre(RESTIFY.pre.sanitizePath());

/*****************************
 * Export the Weaviate module
 * @param  {object} i  - Object that contains all info to start a Weaviate server
 * @return {export} Returns the Weaviate Object
 */
module.exports = (i) => {
    /*****************************
     * Check if all mandatory fields are set
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
    if (global.i.dbHostname === undefined) {
        console.error('DB hostname not set, default to localhost');
        global.i.dbHostname = 'localhost';
    }
    if (global.i.dbPort === undefined) {
        console.error('DB port not set, default to 9160');
        global.i.dbPort = 9160;
    }
    if (global.i.dbContactpoints === undefined) {
        console.error('No Cassandra contactPoints set, default to 127.0.0.1');
        global.i.dbContactpoints = ['127.0.0.1'];
    }
    if (global.i.dbKeyspace === undefined) {
        console.error('You need to set a keyspace name (dbKeyspace) for Cassandra');
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
    if (global.i.dbName === undefined) {
        console.error('Set a db_name value');
    }
    if (global.i.dbPassword === undefined) {
        console.error('Set a db_password value');
    }

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
