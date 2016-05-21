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
     * Deletes an ACL entry.
     * @return {null} null
     */
    getDelete(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with an empty body
            resolve({})
        })
    }

    /**
     * Returns the requested ACL entry.
     * @return {promise} Returns a promise with the correct object
     */
    getGet(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with schema id aclEntry
            resolve({
                cloudAccessRevoked: false,
                creatorEmail: 'foo@bar.computer',
                delegator: 'foo@bar.computer',
                id: '12345-qwerty',
                key: 12345,
                kind: 'weave#aclEntry',
                localAccessInfo: {
                    localAccessEntry: {
                        isApp: true,
                        localAccessRole: 'manager',
                        projectId: 12345
                    },
                    localAuthTokenMintTimeMs: 12345,
                    localAuthTokenTimeLeftMs: 12345,
                    localAuthTokenTtlTimeMs: 12345
                },
                pending: false,
                privileges: 'viewAllEvents',
                revocationTimeMs: 12345,
                role: 'manager',
                scopeId: 'foo@bar.computer',
                scopeMembership: 'manager',
                scopeName: 'some scope name',
                scopePhotoUrl: 'https://url.to/photo.png',
                scopeType: 'application'
            });
        })
    }

    /**
     * Inserts a new ACL entry.
     * @return {promise} Returns a promise with the correct object
     */
    getInsert(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with schema id aclEntry
            resolve({
                cloudAccessRevoked: false,
                creatorEmail: 'foo@bar.computer',
                delegator: 'foo@bar.computer',
                id: '12345-qwerty',
                key: 12345,
                kind: 'weave#aclEntry',
                localAccessInfo: {
                    localAccessEntry: {
                        isApp: true,
                        localAccessRole: 'manager',
                        projectId: 12345
                    },
                    localAuthTokenMintTimeMs: 12345,
                    localAuthTokenTimeLeftMs: 12345,
                    localAuthTokenTtlTimeMs: 12345
                },
                pending: false,
                privileges: 'viewAllEvents',
                revocationTimeMs: 12345,
                role: 'manager',
                scopeId: 'foo@bar.computer',
                scopeMembership: 'manager',
                scopeName: 'some scope name',
                scopePhotoUrl: 'https://url.to/photo.png',
                scopeType: 'application'
            });
        })
    }

    /**
     * Lists ACL entries.
     * @return {promise} Returns a promise with the correct object
     */
    getList(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with special schema AND schema id aclEntry
            resolve({
                kind: "weave#aclEntriesListResponse",
                aclEntries: [{
                    cloudAccessRevoked: false,
                    creatorEmail: 'foo@bar.computer',
                    delegator: 'foo@bar.computer',
                    id: '12345-qwerty',
                    key: 12345,
                    kind: 'weave#aclEntry',
                    localAccessInfo: {
                        localAccessEntry: {
                            isApp: true,
                            localAccessRole: 'manager',
                            projectId: 12345
                        },
                        localAuthTokenMintTimeMs: 12345,
                        localAuthTokenTimeLeftMs: 12345,
                        localAuthTokenTtlTimeMs: 12345
                    },
                    pending: false,
                    privileges: 'viewAllEvents',
                    revocationTimeMs: 12345,
                    role: 'manager',
                    scopeId: 'foo@bar.computer',
                    scopeMembership: 'manager',
                    scopeName: 'some scope name',
                    scopePhotoUrl: 'https://url.to/photo.png',
                    scopeType: 'application'
                }],
                nextPageToken: '12345-qwerty',
                totalResults: 1
            })
        })
    }

    /**
     * Update an ACL entry. This method supports patch semantics.
     * @return {promise} Returns a promise with the correct object
     */
    getPatch(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with schema id aclEntry
            resolve({
                cloudAccessRevoked: false,
                creatorEmail: 'foo@bar.computer',
                delegator: 'foo@bar.computer',
                id: '12345-qwerty',
                key: 12345,
                kind: 'weave#aclEntry',
                localAccessInfo: {
                    localAccessEntry: {
                        isApp: true,
                        localAccessRole: 'manager',
                        projectId: 12345
                    },
                    localAuthTokenMintTimeMs: 12345,
                    localAuthTokenTimeLeftMs: 12345,
                    localAuthTokenTtlTimeMs: 12345
                },
                pending: false,
                privileges: 'viewAllEvents',
                revocationTimeMs: 12345,
                role: 'manager',
                scopeId: 'foo@bar.computer',
                scopeMembership: 'manager',
                scopeName: 'some scope name',
                scopePhotoUrl: 'https://url.to/photo.png',
                scopeType: 'application'
            });
        })
    }

    /**
     * Update an ACL entry.
     * @return {promise} Returns a promise with the correct object
     */
    getUpdate(commandAttributes) {
        return new Promise((resolve, reject) => {
            // resolve with schema id aclEntry
            resolve({
                cloudAccessRevoked: false,
                creatorEmail: 'foo@bar.computer',
                delegator: 'foo@bar.computer',
                id: '12345-qwerty',
                key: 12345,
                kind: 'weave#aclEntry',
                localAccessInfo: {
                    localAccessEntry: {
                        isApp: true,
                        localAccessRole: 'manager',
                        projectId: 12345
                    },
                    localAuthTokenMintTimeMs: 12345,
                    localAuthTokenTimeLeftMs: 12345,
                    localAuthTokenTtlTimeMs: 12345
                },
                pending: false,
                privileges: 'viewAllEvents',
                revocationTimeMs: 12345,
                role: 'manager',
                scopeId: 'foo@bar.computer',
                scopeMembership: 'manager',
                scopeName: 'some scope name',
                scopePhotoUrl: 'https://url.to/photo.png',
                scopeType: 'application'
            });
        })
    }

}
