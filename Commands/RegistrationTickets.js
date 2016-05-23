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

/** Class Commands_RegistrationTickets */
module.exports = class Commands_RegistrationTickets { // Class: Commands_{resources.className}

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
     * Finalizes device registration and returns its credentials. This method may be used only by devices.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getFinalize(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                creationTimeMs: 12345,
                deviceDraft: {},
                deviceId: 'qwerty-12345',
                expirationTimeMs: 12345,
                id: 'qwerty-12345',
                kind: 'weave#registrationTicket',
                oauthClientId: '12345',
                robotAccountAuthorizationCode: 'qwerty-12345',
                robotAccountEmail: 'bleep@bloop.com',
                userEmail: 'bob@weaviate.com'
            });
        });
    }

    /**
     * Returns an existing registration ticket.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getFinalize(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                creationTimeMs: 12345,
                deviceDraft: {},
                deviceId: 'qwerty-12345',
                expirationTimeMs: 12345,
                id: 'qwerty-12345',
                kind: 'weave#registrationTicket',
                oauthClientId: '12345',
                robotAccountAuthorizationCode: 'qwerty-12345',
                robotAccountEmail: 'bleep@bloop.com',
                userEmail: 'bob@weaviate.com'
            });
        });
    }

    /**
     * Creates a new registration ticket.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getInsert(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                creationTimeMs: 12345,
                deviceDraft: {},
                deviceId: 'qwerty-12345',
                expirationTimeMs: 12345,
                id: 'qwerty-12345',
                kind: 'weave#registrationTicket',
                oauthClientId: '12345',
                robotAccountAuthorizationCode: 'qwerty-12345',
                robotAccountEmail: 'bleep@bloop.com',
                userEmail: 'bob@weaviate.com'
            });
        });
    }

    /**
     * Updates an existing registration ticket. This method supports patch semantics.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getPatch(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                creationTimeMs: 12345,
                deviceDraft: {},
                deviceId: 'qwerty-12345',
                expirationTimeMs: 12345,
                id: 'qwerty-12345',
                kind: 'weave#registrationTicket',
                oauthClientId: '12345',
                robotAccountAuthorizationCode: 'qwerty-12345',
                robotAccountEmail: 'bleep@bloop.com',
                userEmail: 'bob@weaviate.com'
            });
        });
    }

    /**
     * Updates an existing registration ticket.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getUpdate(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                creationTimeMs: 12345,
                deviceDraft: {},
                deviceId: 'qwerty-12345',
                expirationTimeMs: 12345,
                id: 'qwerty-12345',
                kind: 'weave#registrationTicket',
                oauthClientId: '12345',
                robotAccountAuthorizationCode: 'qwerty-12345',
                robotAccountEmail: 'bleep@bloop.com',
                userEmail: 'bob@weaviate.com'
            });
        });
    }

};
