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

/** Class Commands_Devices */
module.exports = class Commands_Devices { // Class: Commands_{resources.className}

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
     * Creates client and device local auth tokens to be used by a client locally.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getCreateLocalAuthTokens(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                kind: 'weave#devicesCreateLocalAuthTokensResponse',
                mintedLocalAuthTokens: [
                    {
                        deviceId: '12345-qwerty',
                            deviceToken: 'TOKEN',
                            clientToken: 'TOKEN',
                            localAccessInfo: {
                                localAuthTokenMintTimeMs: 12345,
                                localAuthTokenTtlTimeMs: 12345,
                                localAuthTokenTimeLeftMs: 12345,
                                localAccessEntry: {
                                    localAccessRole: 'foo',
                                    isApp: true,
                                    projectId: 'qwerty-12345'
                                }
                            },
                        retryAfter: 1235
                    }
                ]
            });
        });
    }

    /**
     * Deletes a device from the system.
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
     * Returns a particular device data.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getGet(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                certFingerprint: 'fingerprint',
                channel: {
                    connectionStatusHint: 'online',
                    gcmRegistrationId: 'GCM ID',
                    gcmSenderId: 'SENDER ID',
                    parentId: 'PARENT ID',
                    pubsub: {
                        connectionStatusHint: 'online',
                        topic: 'Pubsub topic'
                    },
                    supportedType: 'xmpp'
                },
                commandDefs: {},
                components: {},
                connectionStatus: 'connection status',
                creationTimeMs: 12345,
                description: 'Description of the device',
                deviceKind: 'Kind of device',
                deviceLocalId: 'Id of the location',
                id: '12345-qwerty',
                invitations: [],
                isEventRecordingDisabled: true,
                kind: 'weave#device',
                lastSeenTimeMs: 12345,
                lastUpdateTimeMs: 12345,
                lastUseTimeMs: 12345,
                location: 'In the Studie',
                modelManifest: {
                    modelName: 'Name of Model',
                    oemName: 'Name of OEM'
                },
                modelManifestId: 'Manifest ID',
                name: 'manufacturer name',
                owner: 'bob@weaviate.com',
                personalizedInfo: {
                    lastUseTimeMs: 12345,
                    location: 'Living Room',
                    maxRole: 'MaxRole',
                    name: 'This is the display name'
                },
                serialNumber: 'AAAAAA',
                state: {},
                stateDefs: {},
                tags: {},
                traits: {},
                uiDeviceKind: 'Kind of device'
            });
        });
    }

    /**
     * Confirms or rejects a pending device.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getHandleInvitation(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({});
        });
    }

    /**
     * Confirms or rejects a pending device.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getInsert(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                certFingerprint: 'fingerprint',
                channel: {
                    connectionStatusHint: 'online',
                    gcmRegistrationId: 'GCM ID',
                    gcmSenderId: 'SENDER ID',
                    parentId: 'PARENT ID',
                    pubsub: {
                        connectionStatusHint: 'online',
                        topic: 'Pubsub topic'
                    },
                    supportedType: 'xmpp'
                },
                commandDefs: {},
                components: {},
                connectionStatus: 'connection status',
                creationTimeMs: 12345,
                description: 'Description of the device',
                deviceKind: 'Kind of device',
                deviceLocalId: 'Id of the location',
                id: '12345-qwerty',
                invitations: [],
                isEventRecordingDisabled: true,
                kind: 'weave#device',
                lastSeenTimeMs: 12345,
                lastUpdateTimeMs: 12345,
                lastUseTimeMs: 12345,
                location: 'In the Studie',
                modelManifest: {
                    modelName: 'Name of Model',
                    oemName: 'Name of OEM'
                },
                modelManifestId: 'Manifest ID',
                name: 'manufacturer name',
                owner: 'bob@weaviate.com',
                personalizedInfo: {
                    lastUseTimeMs: 12345,
                    location: 'Living Room',
                    maxRole: 'MaxRole',
                    name: 'This is the display name'
                },
                serialNumber: 'AAAAAA',
                state: {},
                stateDefs: {},
                tags: {},
                traits: {},
                uiDeviceKind: 'Kind of device'
            });
        });
    }

    /**
     * Lists devices user has access to.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getList(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                kind: 'weave#devicesListResponse',
                devices: [{
                    certFingerprint: 'fingerprint',
                    channel: {
                        connectionStatusHint: 'online',
                        gcmRegistrationId: 'GCM ID',
                        gcmSenderId: 'SENDER ID',
                        parentId: 'PARENT ID',
                        pubsub: {
                            connectionStatusHint: 'online',
                            topic: 'Pubsub topic'
                        },
                        supportedType: 'xmpp'
                    },
                    commandDefs: {},
                    components: {},
                    connectionStatus: 'connection status',
                    creationTimeMs: 12345,
                    description: 'Description of the device',
                    deviceKind: 'Kind of device',
                    deviceLocalId: 'Id of the location',
                    id: '12345-qwerty',
                    invitations: [],
                    isEventRecordingDisabled: true,
                    kind: 'weave#device',
                    lastSeenTimeMs: 12345,
                    lastUpdateTimeMs: 12345,
                    lastUseTimeMs: 12345,
                    location: 'In the Studie',
                    modelManifest: {
                        modelName: 'Name of Model',
                        oemName: 'Name of OEM'
                    },
                    modelManifestId: 'Manifest ID',
                    name: 'manufacturer name',
                    owner: 'bob@weaviate.com',
                    personalizedInfo: {
                        lastUseTimeMs: 12345,
                        location: 'Living Room',
                        maxRole: 'MaxRole',
                        name: 'This is the display name'
                    },
                    serialNumber: 'AAAAAA',
                    state: {},
                    stateDefs: {},
                    tags: {},
                    traits: {},
                    uiDeviceKind: 'Kind of device'
                }],
                nextPageToken: 'ABC',
                totalResults: 1
            });
        });
    }

    /**
     * Updates a device data. This method supports patch semantics.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getPatch(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                certFingerprint: 'fingerprint',
                channel: {
                    connectionStatusHint: 'online',
                    gcmRegistrationId: 'GCM ID',
                    gcmSenderId: 'SENDER ID',
                    parentId: 'PARENT ID',
                    pubsub: {
                        connectionStatusHint: 'online',
                        topic: 'Pubsub topic'
                    },
                    supportedType: 'xmpp'
                },
                commandDefs: {},
                components: {},
                connectionStatus: 'connection status',
                creationTimeMs: 12345,
                description: 'Description of the device',
                deviceKind: 'Kind of device',
                deviceLocalId: 'Id of the location',
                id: '12345-qwerty',
                invitations: [],
                isEventRecordingDisabled: true,
                kind: 'weave#device',
                lastSeenTimeMs: 12345,
                lastUpdateTimeMs: 12345,
                lastUseTimeMs: 12345,
                location: 'In the Studie',
                modelManifest: {
                    modelName: 'Name of Model',
                    oemName: 'Name of OEM'
                },
                modelManifestId: 'Manifest ID',
                name: 'manufacturer name',
                owner: 'bob@weaviate.com',
                personalizedInfo: {
                    lastUseTimeMs: 12345,
                    location: 'Living Room',
                    maxRole: 'MaxRole',
                    name: 'This is the display name'
                },
                serialNumber: 'AAAAAA',
                state: {},
                stateDefs: {},
                tags: {},
                traits: {},
                uiDeviceKind: 'Kind of device'
            });
        });
    }

    /**
     * Applies provided patches to the device state. This method may be used only by devices.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getPatchState(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                state: 'the state'
            });
        });
    }

    /**
     * Applies provided patches to the device state. This method may be used only by devices.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getUpdate(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                certFingerprint: 'fingerprint',
                channel: {
                    connectionStatusHint: 'online',
                    gcmRegistrationId: 'GCM ID',
                    gcmSenderId: 'SENDER ID',
                    parentId: 'PARENT ID',
                    pubsub: {
                        connectionStatusHint: 'online',
                        topic: 'Pubsub topic'
                    },
                    supportedType: 'xmpp'
                },
                commandDefs: {},
                components: {},
                connectionStatus: 'connection status',
                creationTimeMs: 12345,
                description: 'Description of the device',
                deviceKind: 'Kind of device',
                deviceLocalId: 'Id of the location',
                id: '12345-qwerty',
                invitations: [],
                isEventRecordingDisabled: true,
                kind: 'weave#device',
                lastSeenTimeMs: 12345,
                lastUpdateTimeMs: 12345,
                lastUseTimeMs: 12345,
                location: 'In the Studie',
                modelManifest: {
                    modelName: 'Name of Model',
                    oemName: 'Name of OEM'
                },
                modelManifestId: 'Manifest ID',
                name: 'manufacturer name',
                owner: 'bob@weaviate.com',
                personalizedInfo: {
                    lastUseTimeMs: 12345,
                    location: 'Living Room',
                    maxRole: 'MaxRole',
                    name: 'This is the display name'
                },
                serialNumber: 'AAAAAA',
                state: {},
                stateDefs: {},
                tags: {},
                traits: {},
                uiDeviceKind: 'Kind of device'
            });
        });
    }

    /**
     * Updates parent of the child device. Only managers can use this method.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getUpdateParent(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({});
        });
    }

    /**
     * Upserts a device's local auth info.
     * @param {object} commandAttributes  - All attributes needed to exec the command
     * @return {promise} Returns a promise with the correct object
     */
    getUpsertLocalAuthInfo(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with kind and token
            resolve({
                localAuthInfo: {
                    certFingerprint: 'fingerprint',
                    clientToken: 'client token',
                    deviceToken: 'device token',
                    localId: '12345-qwerty',
                    PublicLocalAuthInfo: {
                        certFingerprint: 'fingerprint',
                        localId: 'localId'
                    },
                    revocationClientToken: 'some client token'
                }
            });
        });
    }

};
