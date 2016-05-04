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
module.exports = (i) => {

    /**
     * Set consts
     */
    const   Q = require('q');

    /**
     * Set vars
     */
    var     globalDeferred = Q.defer();

    /**
     * Check if all mandatory fields are set
     */
    if (i === undefined) {
        globalDeferred.reject(new Error('Values aren\'t set when you call Weaviate, please pass an object with proper values. More info on the website'));
    } else if (i.hostname === undefined) {
        globalDeferred.reject(new Error('Hostname not set, default to localhost'));
        i.hostname = 'localhost';
    } else if (i.port === undefined) {
        globalDeferred.reject(new Error('Hostname not set, default to 9000'));
        i.port = 9000;
    } else if (i.formatIn === undefined) {
        globalDeferred.reject(new Error('Format not set, default to JSON'));
        i.formatIn = 'JSON';
    } else if (i.dbHostname === undefined) {
        globalDeferred.reject(new Error('DB hostname not set, default to localhost'));
        i.dbHostname = 'localhost';
    } else if (i.dbPort === undefined) {
        globalDeferred.reject(new Error('DB port not set, default to 9160'));
        i.dbPort = 9160;
    } else if (i.dbContactpoints === undefined) {
        globalDeferred.reject(new Error('No Cassandra contactPoints set, default to 127.0.0.1'));
        i.dbContactpoints = ['127.0.0.1'];
    } else if (i.dbKeyspace === undefined) {
        globalDeferred.reject(new Error('You need to set a keyspace name (dbKeyspace) for Cassandra'));
    } else if (i.formatIn === undefined) {
        globalDeferred.reject(new Error('Format is not set, default to JSON'));
        i.formatIn = 'JSON';
    } else if (i.formatOut === undefined) {
        globalDeferred.reject(new Error('Format is not set, default to JSON'));
        i.formatOut = 'JSON';
    } else if (i.stdoutLog === undefined) {
        globalDeferred.reject(new Error('stdout_log is not set, default to false'));
        i.stdoutLog = false;
    } else if (i.https === undefined) {
        globalDeferred.reject(new Error('https is not set, default to false'));
        i.https = false;
    } else if (i.httpsOpts === undefined && i.https === true) {
        globalDeferred.reject(new Error('You want to use HTTPS, make sure to add https_opts'));
    } else if (i.dbName === undefined) {
        globalDeferred.reject(new Error('Set a db_name value'));
    } else if (i.dbPassword === undefined) {
        globalDeferred.reject(new Error('Set a db_password value'));
    }

    require('./app/clouddevices/v1/view/weaviate.js')(i)
        .done((weaveObject) => {
            globalDeferred.resolve(weaveObject);
        });

    return globalDeferred.promise;

};
