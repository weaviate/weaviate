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

/**
 * HOW DOES IT WORK.
 * Based on: https://developers.google.com/weave/v1/reference/cloud-api/
 *
 *
 * LOGIN USING CREDENTIALS:
 *
 * GOTO URL
 * https://accounts.google.com/o/oauth2/auth?client_id=855446701339-15pbbapet216k9hac60n1sn3ooj1avo4.apps.googleusercontent.com&scope=https://www.googleapis.com/auth/weave.app&response_type=code&redirect_uri=urn:ietf:wg:oauth:2.0:oob
 *
 * GET THE CODE AND USE IT HERE:
 * curl -X POST -F 'code=4/g3MENwl9E9yXhiniS4Pp4UvufJ3RZ46Xdn5dofdW3wA' -F 'client_id=855446701339-15pbbapet216k9hac60n1sn3ooj1avo4.apps.googleusercontent.com' -F 'client_secret=vfoqc4dSHiCg4IYbc3ENv7A_' -F 'redirect_uri=urn:ietf:wg:oauth:2.0:oob' -F 'grant_type=authorization_code' https://accounts.google.com/o/oauth2/token
 *
 * <MODEL MANIFEST>
 * 1st, the commands for the device are being vaildated
 * command: clouddevices.modelManifests.validateCommandDefs
 * It creates a set of commands (see var commandDefs) and example is the _hello command
 *
 * <REGISTRATION TICKETS>
 * 2nd, the device is registred, this means that the PRODUCT is added as a DEVICE for the user.
 * command: clouddevices.registrationTickets.get
 * It sends a useremail and a deviceDraft. Note how the email is the UID for a user.
 *
 * 3th, the registration ticket is patched, a robot email is added
 * command: clouddevices.registrationTickets.patch
 *
 * 4th, the registration ticket is finalized, the device is now setup in the cloud
 * command: clouddevices.registrationTickets.finalize
 *
 * <COMMANDS>
 * 5th, send a command to the device (the _hello command), you can find this command in var commandDefs
 * command: clouddevices.commands.insert
 *
 * 6th, list all the commands send to the device, because we just send one, the result should be an array of 1 (also used as history)
 * command: clouddevices.commands.list
 *
 * 7th, get a specific command
 * command: clouddevices.commands.get
 *
 * 8th, patch a specific command [TEMP DISABLED, UNDERSTAND SCOPE FIRST]
 * command: clouddevices.commands.patch
 *
 * 9th, get the queue of commands that should be executed [TEMP DISABLED, UNDERSTAND SCOPE FIRST]
 * command: clouddevices.commands.getQueue
 *
 * 10th, cancel the command that should be executed [TEMP DISABLED, UNDERSTAND SCOPE FIRST]
 * command: clouddevices.commands.cancel
 *
 * 11th, delete the command that should be executed [TEMP DISABLED, UNDERSTAND SCOPE FIRST]
 * command: clouddevices.commands.delete
 *
 * <DEVICES>
 *
 * 12th, list all devices
 * command: clouddevices.devices.list
 *
 * 13th, get device info
 * command: clouddevices.devices.get
 *
 * 14th, createLocalAuthTokens, creates tokens based on device ID
 * command: clouddevices.devices.createLocalAuthTokens
 *
 * 15th, handleInvitation, handles the invitation of device
 * command: clouddevices.devices.handleInvitation
 *
 * 16th, patch the device
 * command: clouddevices.devices.patch
 *
 * 17th, update the device
 * command: clouddevices.devices.update
 *
 * <EVENTS>
 *
 * 19th, list all events
 * command: clouddevices.events.list
 *
 * 20th, delete all events
 * command: clouddevices.events.deleteAll
 *
 * 21st, record all events is set to true
 * command: clouddevices.events.recordDeviceEvents
 *
 * <DEVICES>
 *
 * 39th, delete the device
 * command: clouddevices.devices.delete
 */
const   assert  = require('assert'),
        request = require('supertest'),
        should  = require('should');

const   weaviateUrl   = 'http://localhost:8080', // google cloud 'https://www.googleapis.com/weave/v1';
        bearer        = 'Bearer ya29.xxx_xxx', // email and bearer should corrolate
        refreshToken  = 'xxx',
        idToken       = 'xxx',
        client_id     = 'xxx',
        client_secret = 'xxx',
        APIKey        = 'xxx'; //created: https://console.cloud.google.com/apis/credentials?project=device-test

var userEmail       = 'bob@weaviate.com'; // email and bearer should corrolate
var modelManifestId = 'AA6GN';
var commandToTest   = '_sample._hello';
var commandToTestParameters = { '_name': 'HELLO EVERYBODY' };
var commandDefs = {
                    '_sample': {
                      '_countdown': {
                        'kind': 'weave#commandDef',
                        'parameters': {
                          '_seconds': {
                            'maximum': 25,
                            'minimum': 1,
                            'type': 'integer'
                          }
                        },
                        'minimalRole': 'user'
                      },
                      '_hello': {
                        'kind': 'weave#commandDef',
                        'parameters': {
                          '_name': {
                            'type': 'string'
                          }
                        },
                        'minimalRole': 'user'
                      },
                      '_ping': {
                        'kind': 'weave#commandDef',
                        'minimalRole': 'user'
                      }
                    },
                    'base': {
                      'updateBaseConfiguration': {
                        'kind': 'weave#commandDef',
                        'parameters': {
                          'localAnonymousAccessMaxRole': {
                            'enum': [
                              'none',
                              'viewer',
                              'user'
                            ],
                            'type': 'string'
                          },
                          'localDiscoveryEnabled': {
                            'type': 'boolean'
                          },
                          'localPairingEnabled': {
                            'type': 'boolean'
                          }
                        },
                        'minimalRole': 'manager'
                      },
                      'updateDeviceInfo': {
                        'kind': 'weave#commandDef',
                        'parameters': {
                          'description': {
                            'type': 'string'
                          },
                          'location': {
                            'type': 'string'
                          },
                          'name': {
                            'type': 'string'
                          }
                        },
                        'minimalRole': 'manager'
                      }
                    }
                };

/**
 * General functions
 */
var counter = 1;

describe('Trying all weave commands', () => {

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.modelManifests.validateCommandDefs', (done) => {
        request(weaviateUrl)
            .post('/modelManifests/validateCommandDefs')
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({ 'commandDefs': commandDefs })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.registrationTickets.get', (done) => {
        request(weaviateUrl)
            .post('/registrationTickets')
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'userEmail': userEmail,
                'deviceDraft': {
                    'deviceKind': 'vendor', // CUSTOM VENDOR
                    'modelManifestId': modelManifestId,
                    'modelManifest': {
                        'modelName': 'Weaviate Test 001',
                        'oemName': 'weaviate-test-001:weaviate-test-001'
                    },
                    'owner': userEmail,
                    'serialNumber': 'TESTCASE123',
                    'name': 'MANUALLY CREATED DEVICE',
                    'description': 'THE DESC OF THE DEVICE',
                    'channel': {
                        'supportedType': 'dev_null',
                        'connectionStatusHint': 'online'
                    },
                    'commandDefs': commandDefs,
                    'state': {
                        '_sample': {
                          '_ping_count': 0
                        },
                        'base': {
                          'firmwareVersion': 'Linux',
                          'localAnonymousAccessMaxRole': 'viewer',
                          'localDiscoveryEnabled': true,
                          'localPairingEnabled': true
                        }
                    }
                }
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#registrationTicket');
                result.should.have.property('id'); // returns custom id
                result.should.have.property('deviceId'); // returns custom id
                result.should.have.property('userEmail', userEmail);
                result.should.have.property('creationTimeMs');
                global.registrationTicketId = result.id;
                global.deviceId = result.deviceId;
                //console.log('NOTE:', 'deviceId', result.deviceId, 'registrationTicketId', result.id);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.registrationTickets.patch', (done) => {
        request(weaviateUrl)
            .patch('/registrationTickets/' + global.registrationTicketId)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .send({
                'robotAccountEmail': 'test@something.com',
            })
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#registrationTicket');
                result.should.have.property('id'); // returns custom id
                result.should.have.property('deviceId', global.deviceId);
                result.should.have.property('userEmail', userEmail);
                result.should.have.property('creationTimeMs');

                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.registrationTickets.finalize', (done) => {
        request(weaviateUrl)
            .post('/registrationTickets/' + global.registrationTicketId + '/finalize')
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('deviceId', global.deviceId);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.aclEntries.insert', (done) => {

        request(weaviateUrl)
            .post('/devices/' + global.deviceId + '/aclEntries')
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'role': 'manager',
                'scopeId': userEmail
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                //console.log('ACL INSERT', result);
                //result.should.have.property('kind', 'weave#eventsListResponse');
                //result.should.have.property('events');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.insert', (done) => {
        request(weaviateUrl)
            .post('/commands')
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'deviceId': global.deviceId,
                'name': commandToTest,
                'parameters': commandToTestParameters
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                //res.status.should.be.equal(200);
                //result.should.have.property('deviceId', global.deviceId);
                //result.should.have.property('creatorEmail', userEmail);
                result.should.have.property('kind', 'weave#command');
                //result.should.have.property('name', commandToTest);
                result.should.have.property('state');
                result.should.have.property('creationTimeMs');
                result.should.have.property('expirationTimeMs');
                result.should.have.property('expirationTimeoutMs');
                global.commandId = result.id;
                //console.log('NOTE:', 'commandId', result.id);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.list', (done) => {
        request(weaviateUrl)
            .get('/commands?deviceId=' + global.deviceId)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#commandsListResponse');
                result.should.have.property('totalResults', 1);
                result = result.commands[0];
                result.should.have.property('kind', 'weave#command');
                //result.should.have.property('id', global.commandId);
                //result.should.have.property('deviceId', global.deviceId);
                //result.should.have.property('creatorEmail', userEmail);
                //result.should.have.property('name', commandToTest);
                result.should.have.property('creationTimeMs');
                result.should.have.property('expirationTimeMs');
                result.should.have.property('expirationTimeoutMs');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.get', (done) => {
        request(weaviateUrl)
            .get('/commands/' + global.commandId)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#command');
                //result.should.have.property('id', global.commandId);
                //result.should.have.property('deviceId', global.deviceId);
                //result.should.have.property('creatorEmail', userEmail);
                //result.should.have.property('name', commandToTest);
                result.should.have.property('creationTimeMs');
                result.should.have.property('expirationTimeMs');
                result.should.have.property('expirationTimeoutMs');

                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.patch', (done) => {
        request(weaviateUrl)
            .patch('/commands/' + global.commandId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                //console.log( JSON.stringify(result) );
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#command');
                //result.should.have.property('id', global.commandId);
                //result.should.have.property('deviceId', global.deviceId);
                //result.should.have.property('creatorEmail', userEmail);
                //result.should.have.property('name', commandToTest);
                result.should.have.property('creationTimeMs');
                result.should.have.property('expirationTimeMs');
                result.should.have.property('expirationTimeoutMs');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.getQueue', (done) => {
        request(weaviateUrl)
            .get('/commands/queue?deviceId=' + global.deviceId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                //console.log(result);
                //result.should.have.property('kind', 'weave#command');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.cancel', (done) => {
        request(weaviateUrl)
            .post('/commands/' + global.commandId + '/cancel' + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                //console.log(result);
                //result.should.have.property('kind', 'weave#command');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.commands.delete', (done) => {
        request(weaviateUrl)
            .delete('/commands/' + global.commandId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                //console.log(result);
                //result.should.have.property('kind', 'weave#command');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.list', (done) => {
        request(weaviateUrl)
            .get('/devices?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#devicesListResponse');
                result.should.have.property('devices');
                result.should.have.property('totalResults');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.get', (done) => {
        request(weaviateUrl)
            .get('/devices/' + global.deviceId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#device');
                //result.should.have.property('id', global.deviceId);
                result.should.have.property('deviceKind');
                //result.should.have.property('modelManifestId', modelManifestId);
                result.should.have.property('serialNumber');
                result.should.have.property('creationTimeMs');
                result.should.have.property('lastUpdateTimeMs');
                result.should.have.property('lastUseTimeMs');
                result.should.have.property('lastSeenTimeMs');
                result.should.have.property('name');
                result.should.have.property('description');
                result.should.have.property('owner');
                result.should.have.property('uiDeviceKind');
                result.should.have.property('modelManifest');
                result.should.have.property('commandDefs');
                result.should.have.property('state');
                result.should.have.property('connectionStatus');
                result.should.have.property('channel');
                result.should.have.property('personalizedInfo');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.createLocalAuthTokens', (done) => {
        request(weaviateUrl)
            .post('/devices/createLocalAuthTokens?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'deviceIds': [ global.deviceId ]
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#devicesCreateLocalAuthTokensResponse');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.handleInvitation', (done) => {
        request(weaviateUrl)
            .post('/devices/' + global.deviceId + '/handleInvitation?action=accept&key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.patch', (done) => {
        request(weaviateUrl)
            .patch('/devices/' + global.deviceId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'description': 'An patched description'
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                console.log(result);
                result.should.have.property('kind', 'weave#device');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.update', (done) => {
        request(weaviateUrl)
            .put('/devices/' + global.deviceId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'description': 'An patched description'
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#device');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.events.list', (done) => {

        request(weaviateUrl)
            .get('/events?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                result.should.have.property('kind', 'weave#eventsListResponse');
                result.should.have.property('events');
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.events.deleteAll', (done) => {

        request(weaviateUrl)
            .post('/events/deleteAll?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'deviceId': global.deviceId
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.events.recordDeviceEvents', (done) => {

        request(weaviateUrl)
            .post('/events/recordDeviceEvents?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .send({
                'deviceId': global.deviceId,
                'recordDeviceEvents': true
            })
            .expect('Content-Type', /json/)
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                done();
            });
    });

    /********************************************************************************************/

    it((counter++) + '/39 clouddevices.devices.delete', (done) => {
        request(weaviateUrl)
            .delete('/devices/' + global.deviceId + '?key=' + APIKey)
            .set('Authorization', bearer)
            .set('Accept', 'application/json')
            .expect(200)
            .end(function (err, res) {
                let result = res.body;
                res.status.should.be.equal(200);
                done();
            });
    });

});
