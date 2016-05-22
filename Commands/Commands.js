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
module.exports = class Commands_Commands { // Class: Commands_{resources.className}

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
     * Cancels a command.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getCancel(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                blobParameters: {},
                blobResults: {},
                component: 'component_a/component_b',
                creationTimeMs: 12345,
                creatorEmail: 'bob@weaviate.com',
                deviceId: '12345-qwerty',
                error: {
                    arguments: {
                        ['argument_a', 'argument_b']
                    },
                    code: 123,
                    message: 'message that goes with the code'
                },
                expirationTimeMs: 12345,
                expirationTimeoutMs: 12345,
                id: '12345-qwerty',
                kind: 'weave#command',
                name: 'name of the command',
                parameters: {},
                progress: {},
                results: {},
                state: 'done',
                userAction: 'some user action'
            });
        });
    }

    /**
     * Deletes a command.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getDelete(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({});
        });
    }

    /**
     * Returns a particular command.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getGet(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                blobParameters: {},
                blobResults: {},
                component: 'component_a/component_b',
                creationTimeMs: 12345,
                creatorEmail: 'bob@weaviate.com',
                deviceId: '12345-qwerty',
                error: {
                    arguments: {
                        ['argument_a', 'argument_b']
                    },
                    code: 123,
                    message: 'message that goes with the code'
                },
                expirationTimeMs: 12345,
                expirationTimeoutMs: 12345,
                id: '12345-qwerty',
                kind: 'weave#command',
                name: 'name of the command',
                parameters: {},
                progress: {},
                results: {},
                state: 'done',
                userAction: 'some user action'
            });
        });
    }

    /**
     * Returns queued commands that device is supposed to execute. This method may be used only by devices.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getQueue(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                commands: [
                    {
                        blobParameters: {},
                        blobResults: {},
                        component: 'component_a/component_b',
                        creationTimeMs: 12345,
                        creatorEmail: 'bob@weaviate.com',
                        deviceId: '12345-qwerty',
                        error: {
                            arguments: {
                                ['argument_a', 'argument_b']
                            },
                            code: 123,
                            message: 'message that goes with the code'
                        },
                        expirationTimeMs: 12345,
                        expirationTimeoutMs: 12345,
                        id: '12345-qwerty',
                        kind: 'weave#command',
                        name: 'name of the command',
                        parameters: {},
                        progress: {},
                        results: {},
                        state: 'done',
                        userAction: 'some user action'
                    }
                ]
            });
        });
    }

    /**
     * Creates and sends a new command.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getInsert(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                blobParameters: {},
                blobResults: {},
                component: 'component_a/component_b',
                creationTimeMs: 12345,
                creatorEmail: 'bob@weaviate.com',
                deviceId: '12345-qwerty',
                error: {
                    arguments: {
                        ['argument_a', 'argument_b']
                    },
                    code: 123,
                    message: 'message that goes with the code'
                },
                expirationTimeMs: 12345,
                expirationTimeoutMs: 12345,
                id: '12345-qwerty',
                kind: 'weave#command',
                name: 'name of the command',
                parameters: {},
                progress: {},
                results: {},
                state: 'done',
                userAction: 'some user action'
            });
        });
    }

    /**
     * Lists all commands in reverse order of creation.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getList(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                kind: 'weave#commandsListResponse',
                commands: [
                    {
                        blobParameters: {},
                        blobResults: {},
                        component: 'component_a/component_b',
                        creationTimeMs: 12345,
                        creatorEmail: 'bob@weaviate.com',
                        deviceId: '12345-qwerty',
                        error: {
                            arguments: {
                                ['argument_a', 'argument_b']
                            },
                            code: 123,
                            message: 'message that goes with the code'
                        },
                        expirationTimeMs: 12345,
                        expirationTimeoutMs: 12345,
                        id: '12345-qwerty',
                        kind: 'weave#command',
                        name: 'name of the command',
                        parameters: {},
                        progress: {},
                        results: {},
                        state: 'done',
                        userAction: 'some user action'
                    }
                  ],
                  nextPageToken: '12345-qwerty',
                  totalResults: 1
                }
            );
        });
    }

    /**
     * Updates a command. This method may be used only by devices. This method supports patch semantics.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getPatch(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                blobParameters: {},
                blobResults: {},
                component: 'component_a/component_b',
                creationTimeMs: 12345,
                creatorEmail: 'bob@weaviate.com',
                deviceId: '12345-qwerty',
                error: {
                    arguments: {
                        ['argument_a', 'argument_b']
                    },
                    code: 123,
                    message: 'message that goes with the code'
                },
                expirationTimeMs: 12345,
                expirationTimeoutMs: 12345,
                id: '12345-qwerty',
                kind: 'weave#command',
                name: 'name of the command',
                parameters: {},
                progress: {},
                results: {},
                state: 'done',
                userAction: 'some user action'
            });
    }

};
