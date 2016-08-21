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

/** Class Commands_AuthorizedApps */
module.exports = class Commands_AuthorizedApps { // Class: Commands_{resources.className}

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
     * Generate a token used to authenticate an authorized app.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $CreateAppAuthenticationToken(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
              kind: 'weave#authorizedAppsCreateAppAuthenticationTokenResponse',
              token: '12345-qwerty'
            });
        });
    }

    /**
     * The actual list of authorized apps.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    $List(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
              kind: 'weave#authorizedAppsListResponse',
              authorizedApps: [
                {
                  kind: 'weave#authorizedApp',
                  projectId: '12345-qwerty',
                  displayName: 'Some Display Name',
                  iconUrl: 'https://my.url/photo.png',
                  androidApps: [
                    {
                      package_name: 'some package name',
                      certificate_hash: 'some certificate hash'
                    }
                  ]
                }
              ]
            });
        });
    }

};
