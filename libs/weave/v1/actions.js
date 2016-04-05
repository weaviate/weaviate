'use strict';
/**                         _       _       
 *                         (_)     | |      
 *__      _____  __ ___   ___  __ _| |_ ___ 
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *                                          
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/LICENSE
 * See www.weaviate.com for details
 * See package.json for auther and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
module.exports = {

    /**
     * The validateBodyObject checks if all the mandatory fields are set in the body
     *
     * @param  weaveObject  This is the body object
     * @param  this is an array of mandatory fields
     * @param  callback is the callback, return true if all fields are included, returns false if not
     */
    validateBodyObject: (bodyObject, mandatoryArray, callback) => {
        /**
         * synchronously check if all values are in the body object
         */
        mandatoryArray
            .forEach((val) => {
                switch (val in bodyObject) {
                    case false:
                    callback(false);
                }
            });
        /**
         * False never passed, so callback send true
         */
        callback(true);
    },

    /**
     * returns false if wrong and object if correct
     */
    process: (action, returnValuesArray, callback) => {

        switch (action) {
            /**
             *
             */
            case 'weave.aclEntries.delete':
                callback({ ok: 'ok' })

            /**
             *
             */
            case 'weave.aclEntries.get':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.aclEntries.patch':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.aclEntries.update':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.aclEntries.insert':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.aclEntries.list':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.authorizedApps.createappauthenticationtoken':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.authorizedApps.list':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.cancel':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.delete':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.get':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.patch':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.update':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.getQueue':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.insert':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.commands.list':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.delete':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.get':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.patch':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.update':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.handleInvitation':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.insert':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.list':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.patchState':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.devices.updateParent':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.events.list':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.modelManifests.get':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.modelManifests.list':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.modelManifests.validateCommandDefs':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.modelManifests.validateDeviceState':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.personalizedInfos.get':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.personalizedInfos.patch':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.personalizedInfos.update':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.registrationTickets.finalize':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.registrationTickets.get':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.registrationTickets.patch':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.registrationTickets.update':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.registrationTickets.insert':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            case 'weave.subscriptions.subscribe':
                callback({ ok: 'ok' })
                
            /**
             *
             */
            default:
                callback(false)
        }
    }
}