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

/** Class Commands_AclEntries */
module.exports = class Commands_AclEntries { // Class: Commands_{resources.className}

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
     * Used by an adapter provider to accept an activation and prevent it from expiring.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {null} null
     */
    $Accept(commandAttributes) {
        return new Promise((resolve, reject) => {
            resolve({
              kind: 'weave#adaptersAcceptResponse'
            });
        });
    }

    /**
     * Activates an adapter. The activation will be contingent upon the adapter provider accepting the activation. If the activation is not accepted within 15 minutes, the activation will be deleted.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {null} null
     */
    $Activate(commandAttributes) {
        return new Promise((resolve, reject) => {
            resolve({
              kind: 'weave#adaptersActivateResponse',
              activationId: 'string',
              activationUrl: 'string'
            });
        });
    }

    /**
     * Deactivates an adapter. This will also delete all devices provided by that adapter.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {null} null
     */
    $Deactivate(commandAttributes) {
        return new Promise((resolve, reject) => {
            resolve({
              kind: 'weave#adaptersDeactivateResponse',
              deletedCount: 1
            });
        });
    }

    /**
     * Get an adapter.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {null} null
     */
    $Get(commandAttributes) {
        return new Promise((resolve, reject) => {
            resolve({
              id: 123,
              displayName: 'name',
              iconUrl: 'https://icon.com/',
              activateUrl: 'https://activate.com',
              manageUrl: 'https://manage.com',
              deactivateUrl: 'https://deactivate.com',
              activated: true
            });
        });
    }

    /**
     * Lists adapters.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {null} null
     */
    $List(commandAttributes) {
        return new Promise((resolve, reject) => {
            resolve({
              kind: 'weave#adaptersListResponse',
              adapters: [{}]
            });
        });
    }

};
