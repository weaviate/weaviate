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
const ACTIONS = require('../controller/actions.js');
module.exports = {
    /**
     * delete
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    delete: (i, weaveObject, Q) => {
      var deferred = Q.defer(); // no repsonse needed
      try {
            /**
             * Validate if the provide body is correct, if no body is expected, keep the array empty []
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.aclEntries.delete', [], (processResult) => {
                        switch (processResult) {
                          case false:
                              deferred.reject('Something processing this request went wrong');
                          default:
                              deferred.resolve({});
                          }
                    });
                  break;
              default:
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
            deferred.reject(error);
        }
      return deferred.promise;
  },
    /**
     * get
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    get: (i, weaveObject, Q) => {
      var deferred = Q.defer();
      try {
            /**
             * Validate if the provide body is correct
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.aclEntries.get', [
                            /**
                             * description  boolean
                             * type  Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.
                             */
                        'cloudAccessRevoked',
                            /**
                             * description  string
                             * type  User who created this entry. At the moment it is populated only when pending == true.
                             */
                        'creatorEmail',
                            /**
                             * description  string
                             * type  User on behalf of whom the access is granted to the application.
                             */
                        'delegator',
                            /**
                             * description  string
                             * type  Unique ACL entry ID.
                             */
                        'id',
                            /**
                             * description  string
                             * type  Public access key value. Set only when scopeType is PUBLIC.
                             * format  int64
                             */
                        'key',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#aclEntry".
                             */
                        'kind',
                            /**
                             * description  any
                             * type  Information about local auth tokens timestamps.
                             */
                        'localAccessInfo',
                            /**
                             * description  boolean
                             * type  Whether this ACL entry is pending for user reply to accept/reject it.
                             */
                        'pending',
                            /**
                             * description  array
                             * type  Set of access privileges granted for this scope.

                            Valid values are:
                            - "modifyAcl"
                            - "viewAllEvents"
                             */
                        'privileges',
                            /**
                             * description  string
                             * type  Access role granted to this scope.
                             * enum  manager, owner, robot, user, viewer
                             * required  clouddevices.aclEntries.insert
                             */
                        'role',
                            /**
                             * description  string
                             * type  Email address if scope type is user or group, domain name if scope type is a domain.
                             * required  clouddevices.aclEntries.insert
                             */
                        'scopeId',
                            /**
                             * description  string
                             * type  Type of membership the user has in the scope.
                             * enum  delegator, manager, member, none
                             */
                        'scopeMembership',
                            /**
                             * description  string
                             * type  Displayable scope name.
                             */
                        'scopeName',
                            /**
                             * description  string
                             * type  URL of this scope displayable photo.
                             */
                        'scopePhotoUrl',
                            /**
                             * description  string
                             * type  Type of the access scope.
                             * enum  application, domain, group, public, user
                             */
                        'scopeType'
                    ], (processResult) => {
                            switch (processResult) {
                          case false:
                              deferred.reject('Something processing this request went wrong');
                          default:
                              deferred.resolve(processResult);
                          }
                        });
                  break;
              default:
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
            deferred.reject(error);
        }
      return deferred.promise;
  },
    /**
     * patch
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    patch: (i, weaveObject, Q) => {
      var deferred = Q.defer();
      try {
            /**
             * Validate if the provide body is correct
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.aclEntries.patch', [
                            /**
                             * description  boolean
                             * type  Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.
                             */
                        'cloudAccessRevoked',
                            /**
                             * description  string
                             * type  User who created this entry. At the moment it is populated only when pending == true.
                             */
                        'creatorEmail',
                            /**
                             * description  string
                             * type  User on behalf of whom the access is granted to the application.
                             */
                        'delegator',
                            /**
                             * description  string
                             * type  Unique ACL entry ID.
                             */
                        'id',
                            /**
                             * description  string
                             * type  Public access key value. Set only when scopeType is PUBLIC.
                             * format  int64
                             */
                        'key',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#aclEntry".
                             */
                        'kind',
                            /**
                             * description  any
                             * type  Information about local auth tokens timestamps.
                             */
                        'localAccessInfo',
                            /**
                             * description  boolean
                             * type  Whether this ACL entry is pending for user reply to accept/reject it.
                             */
                        'pending',
                            /**
                             * description  array
                             * type  Set of access privileges granted for this scope.

                            Valid values are:
                            - "modifyAcl"
                            - "viewAllEvents"
                             */
                        'privileges',
                            /**
                             * description  string
                             * type  Access role granted to this scope.
                             * enum  manager, owner, robot, user, viewer
                             * required  clouddevices.aclEntries.insert
                             */
                        'role',
                            /**
                             * description  string
                             * type  Email address if scope type is user or group, domain name if scope type is a domain.
                             * required  clouddevices.aclEntries.insert
                             */
                        'scopeId',
                            /**
                             * description  string
                             * type  Type of membership the user has in the scope.
                             * enum  delegator, manager, member, none
                             */
                        'scopeMembership',
                            /**
                             * description  string
                             * type  Displayable scope name.
                             */
                        'scopeName',
                            /**
                             * description  string
                             * type  URL of this scope displayable photo.
                             */
                        'scopePhotoUrl',
                            /**
                             * description  string
                             * type  Type of the access scope.
                             * enum  application, domain, group, public, user
                             */
                        'scopeType'
                    ], (processResult) => {
                            switch (processResult) {
                          case false:
                              deferred.reject('Something processing this request went wrong');
                          default:
                              deferred.resolve(processResult);
                          }
                        });
                  break;
              default:
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
            deferred.reject(error);
        }
      return deferred.promise;
  },
    /**
     * update
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    update: (i, weaveObject, Q) => {
      var deferred = Q.defer();
      try {
            /**
             * Validate if the provide body is correct
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.aclEntries.update', [
                            /**
                             * description  boolean
                             * type  Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.
                             */
                        'cloudAccessRevoked',
                            /**
                             * description  string
                             * type  User who created this entry. At the moment it is populated only when pending == true.
                             */
                        'creatorEmail',
                            /**
                             * description  string
                             * type  User on behalf of whom the access is granted to the application.
                             */
                        'delegator',
                            /**
                             * description  string
                             * type  Unique ACL entry ID.
                             */
                        'id',
                            /**
                             * description  string
                             * type  Public access key value. Set only when scopeType is PUBLIC.
                             * format  int64
                             */
                        'key',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#aclEntry".
                             */
                        'kind',
                            /**
                             * description  any
                             * type  Information about local auth tokens timestamps.
                             */
                        'localAccessInfo',
                            /**
                             * description  boolean
                             * type  Whether this ACL entry is pending for user reply to accept/reject it.
                             */
                        'pending',
                            /**
                             * description  array
                             * type  Set of access privileges granted for this scope.

                            Valid values are:
                            - "modifyAcl"
                            - "viewAllEvents"
                             */
                        'privileges',
                            /**
                             * description  string
                             * type  Access role granted to this scope.
                             * enum  manager, owner, robot, user, viewer
                             * required  clouddevices.aclEntries.insert
                             */
                        'role',
                            /**
                             * description  string
                             * type  Email address if scope type is user or group, domain name if scope type is a domain.
                             * required  clouddevices.aclEntries.insert
                             */
                        'scopeId',
                            /**
                             * description  string
                             * type  Type of membership the user has in the scope.
                             * enum  delegator, manager, member, none
                             */
                        'scopeMembership',
                            /**
                             * description  string
                             * type  Displayable scope name.
                             */
                        'scopeName',
                            /**
                             * description  string
                             * type  URL of this scope displayable photo.
                             */
                        'scopePhotoUrl',
                            /**
                             * description  string
                             * type  Type of the access scope.
                             * enum  application, domain, group, public, user
                             */
                        'scopeType'
                    ], (processResult) => {
                            switch (processResult) {
                          case false:
                              deferred.reject('Something processing this request went wrong');
                          default:
                              deferred.resolve(processResult);
                          }
                        });
                  break;
              default:
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
            deferred.reject(error);
        }
      return deferred.promise;
  },
    /**
     * insert
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    insert: (i, weaveObject, Q) => {
      var deferred = Q.defer();
      try {
            /**
             * Validate if the provide body is correct
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.aclEntries.insert', [
                            /**
                             * description  boolean
                             * type  Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.
                             */
                        'cloudAccessRevoked',
                            /**
                             * description  string
                             * type  User who created this entry. At the moment it is populated only when pending == true.
                             */
                        'creatorEmail',
                            /**
                             * description  string
                             * type  User on behalf of whom the access is granted to the application.
                             */
                        'delegator',
                            /**
                             * description  string
                             * type  Unique ACL entry ID.
                             */
                        'id',
                            /**
                             * description  string
                             * type  Public access key value. Set only when scopeType is PUBLIC.
                             * format  int64
                             */
                        'key',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#aclEntry".
                             */
                        'kind',
                            /**
                             * description  any
                             * type  Information about local auth tokens timestamps.
                             */
                        'localAccessInfo',
                            /**
                             * description  boolean
                             * type  Whether this ACL entry is pending for user reply to accept/reject it.
                             */
                        'pending',
                            /**
                             * description  array
                             * type  Set of access privileges granted for this scope.

                            Valid values are:
                            - "modifyAcl"
                            - "viewAllEvents"
                             */
                        'privileges',
                            /**
                             * description  string
                             * type  Access role granted to this scope.
                             * enum  manager, owner, robot, user, viewer
                             * required  clouddevices.aclEntries.insert
                             */
                        'role',
                            /**
                             * description  string
                             * type  Email address if scope type is user or group, domain name if scope type is a domain.
                             * required  clouddevices.aclEntries.insert
                             */
                        'scopeId',
                            /**
                             * description  string
                             * type  Type of membership the user has in the scope.
                             * enum  delegator, manager, member, none
                             */
                        'scopeMembership',
                            /**
                             * description  string
                             * type  Displayable scope name.
                             */
                        'scopeName',
                            /**
                             * description  string
                             * type  URL of this scope displayable photo.
                             */
                        'scopePhotoUrl',
                            /**
                             * description  string
                             * type  Type of the access scope.
                             * enum  application, domain, group, public, user
                             */
                        'scopeType'
                    ], (processResult) => {
                            switch (processResult) {
                          case false:
                              deferred.reject('Something processing this request went wrong');
                          default:
                              deferred.resolve(processResult);
                          }
                        });
                  break;
              default:
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
            deferred.reject(error);
        }
      return deferred.promise;
  },
    /**
     * list
     * @param   {string} i input URL
     * @param   {object} weaveObject OBJ Object with the send in body and params
     * @param   {object} Q Defer object
     * @returns {object} deferred.resolve or deferred.reject
     */
    list: (i, weaveObject, Q) => {
      var deferred = Q.defer();
      try {
            /**
             * Validate if the provide body is correct
             */
          ACTIONS.validateBodyObject(weaveObject, [], (result) => {
              switch (result) {
              case true:
                        /**
                         * Provided body is correct, handle the request
                         */
                  ACTIONS.process('clouddevices.aclEntries.list', [
                            /**
                             * description  array
                             * type  The actual list of ACL entries.
                             */
                        'aclEntries',
                            /**
                             * description  string
                             * type  Identifies what kind of resource this is. Value: the fixed string "clouddevices#aclEntriesListResponse".
                             */
                        'kind',
                            /**
                             * description  string
                             * type  Token corresponding to the next page of ACL entries.
                             */
                        'nextPageToken',
                            /**
                             * description  integer
                             * type  The total number of ACL entries for the query. The number of items in a response may be smaller due to paging.
                             * format  int32
                             */
                        'totalResults'
                    ], (processResult) => {
                            switch (processResult) {
                          case false:
                              deferred.reject('Something processing this request went wrong');
                          default:
                              deferred.resolve(processResult);
                          }
                        });
                  break;
              default:
                        /**
                         * Provided body is incorrect, send error
                         */
                  deferred.reject('Provided body is incorrect');
                  break;
              }
          });
      } catch (error) {
            deferred.reject(error);
        }
      return deferred.promise;
  }
};
