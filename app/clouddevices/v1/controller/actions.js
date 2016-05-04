'use strict';
/*                          _       _
 *                         (_)     | |
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

const actionsDevices = require('./devices.js');

module.exports = {

  /**
   * The validateBodyObject          Checks if all the mandatory fields are set in the body
   * @param {Object} weaveObject     This is the body object
   * @param {Array}  mandatoryArray  This is an array of mandatory fields
   * @param {Object} callback        Callback is the callback, return true if all fields are included, returns false if not
   * @returns {void} nothing
   */
    validateBodyObject: (weaveObject, mandatoryArray, callback) => {
    /**
     * synchronously check if all values are in the body object
     */
      mandatoryArray
            .forEach((val) => {
                switch (val in weaveObject) {
              case false:
                  callback(false);
              default:
                  callback(true);
              }
            });
        /**
         * False never passed, so callback send true
         */
      callback(true);
  },
  /**
   * Process returns false if wrong and object if correct
   * @param {string} action             The actions
   * @param {Object} callback           Callback function
   * @returns {void} nothing
   */
    process: (action, callback) => {

      switch (action) {
            /**
             * actions_aclEntries.delete()
             */
    case 'clouddevices.aclEntries.delete':
        callback({ ok: 'clouddevices.aclEntries.delete' });
        break;

            /**
             *
             */
    case 'clouddevices.aclEntries.get':
        callback({ ok: 'clouddevices.aclEntries.get' });
        break;

            /**
             *
             */
    case 'clouddevices.aclEntries.patch':
        callback({ ok: 'clouddevices.aclEntries.patch' });
        break;

            /**
             *
             */
    case 'clouddevices.aclEntries.update':
        callback({ ok: 'clouddevices.aclEntries.update' });
        break;

            /**
             *
             */
    case 'clouddevices.aclEntries.insert':
        callback({ ok: 'clouddevices.aclEntries.insert' });
        break;

            /**
             *
             */
    case 'clouddevices.aclEntries.list':
        callback({ ok: 'clouddevices.aclEntries.list' });
        break;

            /**
             *
             */
    case 'clouddevices.authorizedApps.createappauthenticationtoken':
        callback({ ok: 'clouddevices.authorizedApps.createappauthenticationtoken' });
        break;

            /**
             *
             */
    case 'clouddevices.authorizedApps.list':
        callback({ ok: 'clouddevices.authorizedApps.list' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.cancel':
        callback({ ok: 'clouddevices.commands.cancel' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.delete':
        callback({ ok: 'clouddevices.commands.delete' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.get':
        callback({ ok: 'clouddevices.commands.get' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.patch':
        callback({ ok: 'clouddevices.commands.patch' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.update':
        callback({ ok: 'clouddevices.commands.update' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.getQueue':
        callback({ ok: 'clouddevices.commands.getQueue' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.insert':
        callback({ ok: 'clouddevices.commands.insert' });
        break;

            /**
             *
             */
    case 'clouddevices.commands.list':
        callback({ ok: 'clouddevices.commands.list' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.delete':
        callback({ ok: 'clouddevices.devices.delete' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.get':
        callback({ ok: 'clouddevices.devices.get' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.patch':
        callback({ ok: 'clouddevices.devices.patch' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.update':
        callback({ ok: 'clouddevices.devices.update' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.handleInvitation':
        callback({ ok: 'clouddevices.devices.handleInvitation' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.insert':
        callback({ ok: 'clouddevices.devices.insert' });
        break;

        /**
         * List all the devices
         */
    case 'clouddevices.devices.list':
        actionsDevices.list(returnValuesArray, (returnObject) => {
              callback(returnObject);
          });
        break;

            /**
             *
             */
    case 'clouddevices.devices.patchState':
        callback({ ok: 'clouddevices.devices.patchState' });
        break;

            /**
             *
             */
    case 'clouddevices.devices.updateParent':
        callback({ ok: 'clouddevices.devices.updateParent' });
        break;

            /**
             *
             */
    case 'clouddevices.events.list':
        callback({ ok: 'clouddevices.events.list' });
        break;

            /**
             *
             */
    case 'clouddevices.modelManifests.get':
        callback({ ok: 'clouddevices.modelManifests.get' });
        break;

            /**
             *
             */
    case 'clouddevices.modelManifests.list':
        callback({ ok: 'clouddevices.modelManifests.list' });
        break;

            /**
             *
             */
    case 'clouddevices.modelManifests.validateCommandDefs':
        callback({ ok: 'clouddevices.modelManifests.validateCommandDefs' });
        break;

            /**
             *
             */
    case 'clouddevices.modelManifests.validateDeviceState':
        callback({ ok: 'clouddevices.modelManifests.validateDeviceState' });
        break;

            /**
             *
             */
    case 'clouddevices.personalizedInfos.get':
        callback({ ok: 'clouddevices.personalizedInfos.get' });
        break;

            /**
             *
             */
    case 'clouddevices.personalizedInfos.patch':
        callback({ ok: 'clouddevices.personalizedInfos.patch' });
        break;

            /**
             *
             */
    case 'clouddevices.personalizedInfos.update':
        callback({ ok: 'clouddevices.personalizedInfos.update' });
        break;

            /**
             *
             */
    case 'clouddevices.registrationTickets.finalize':
        callback({ ok: 'clouddevices.registrationTickets.finalize' });
        break;

            /**
             *
             */
    case 'clouddevices.registrationTickets.get':
        callback({ ok: 'clouddevices.registrationTickets.get' });
        break;

            /**
             *
             */
    case 'clouddevices.registrationTickets.patch':
        callback({ ok: 'clouddevices.registrationTickets.patch' });
        break;

            /**
             *
             */
    case 'clouddevices.registrationTickets.update':
        callback({ ok: 'clouddevices.registrationTickets.update' });
        break;

            /**
             *
             */
    case 'clouddevices.registrationTickets.insert':
        callback({ ok: 'clouddevices.registrationTickets.insert' });
        break;

            /**
             *
             */
    case 'clouddevices.subscriptions.subscribe':
        callback({ ok: 'clouddevices.subscriptions.subscribe' });
        break;

            /**
             *
             */
    default:
        callback(false);
        break;
    }
  }
};
