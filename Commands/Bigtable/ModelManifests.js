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

/** Class Commands_ModelManifeststs */
module.exports = class Commands_ModelManifeststs { // Class: Commands_{resources.className}

    /**
     * Constructor for this Command
     * @param {object} req  - The request
     * @param {object} res  - The response
     * @param {object} next - Next() function
     */
    constructor(req, res, next) {
        this.req  = req;
        this.res  = res;
        this.next = next;
    }

    /**
     * Returns a particular model manifest.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $Get(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                'allowedChildModelManifestIds': ['aaa', 'bbb'],
                'applications': [],
                'confirmationImageUrl': 'http://image.foo/bar.jpg',
                'deviceImageUrl': 'http://image.foo/bar.jpg',
                'deviceKind': 'camera',
                'id': 'qwerty-12345',
                'kind': 'weave#modelManifest',
                'modelDescription': 'desc of the model',
                'modelName': 'name of the model',
                'oemName': 'AAAAAA',
                'supportPageUrl': 'http://www.test.com/'
            });
        });
    }

    /**
     * Lists all model manifests.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $List(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                kind: 'weave#modelManifestsListResponse',
                modelManifests: [{
                    'allowedChildModelManifestIds': ['aaa', 'bbb'],
                    'applications': [],
                    'confirmationImageUrl': 'http://image.foo/bar.jpg',
                    'deviceImageUrl': 'http://image.foo/bar.jpg',
                    'deviceKind': 'camera',
                    'id': 'qwerty-12345',
                    'kind': 'weave#modelManifest',
                    'modelDescription': 'desc of the model',
                    'modelName': 'name of the model',
                    'oemName': 'AAAAAA',
                    'supportPageUrl': 'http://www.test.com/'
                }],
                nextPageToken: 'abc-123',
                totalResults: 1
            });
        });
    }

    /**
     * Validates given command definitions and returns errors.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $ValidateCommandDefs(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                validationErrors: []
            });
        });
    }

    /**
     * Validates given components definitions and returns errors.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $ValidateComponents(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                validationErrors: []
            });
        });
    }

    /**
     * Validates given device state object and returns errors.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $ValidateDeviceState(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                state: {}
            });
        });
    }

};
