'use strict';
module.exports = {
	'schemas': {
		'AclEntriesListResponse': {
			'id': 'AclEntriesListResponse',
			'type': 'object',
			'description': 'List of Access control list entries.',
			'externalTypeName': 'clouddevices.api.AclEntriesResponse',
			'properties': {
				'aclEntries': {
					'type': 'array',
					'description': 'The actual list of ACL entries.',
					'items': {
						'$ref': 'AclEntry'
					}
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#aclEntriesListResponse\'.',
					'default': 'clouddevices#aclEntriesListResponse'
				},
				'nextPageToken': {
					'type': 'string',
					'description': 'Token corresponding to the next page of ACL entries.'
				},
				'totalResults': {
					'type': 'integer',
					'description': 'The total number of ACL entries for the query. The number of items in a response may be smaller due to paging.',
					'format': 'int32'
				}
			}
		},
		'AclEntry': {
			'id': 'AclEntry',
			'type': 'object',
			'properties': {
				'cloudAccessRevoked': {
					'type': 'boolean',
					'description': 'Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details.'
				},
				'creatorEmail': {
					'type': 'string',
					'description': 'User who created this entry. At the moment it is populated only when pending == true.'
				},
				'delegator': {
					'type': 'string',
					'description': 'User on behalf of whom the access is granted to the application.'
				},
				'id': {
					'type': 'string',
					'description': 'Unique ACL entry ID.'
				},
				'key': {
					'type': 'string',
					'description': 'Public access key value. Set only when scopeType is PUBLIC.',
					'format': 'int64'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#aclEntry\'.',
					'default': 'clouddevices#aclEntry'
				},
				'localAccessInfo': {
					'type': 'any',
					'description': 'Information about local auth tokens timestamps.'
				},
				'pending': {
					'type': 'boolean',
					'description': 'Whether this ACL entry is pending for user reply to accept/reject it.'
				},
				'privileges': {
					'type': 'array',
					'description': 'Set of access privileges granted for this scope.\n\nValid values are:  \n- \'modifyAcl\' \n- \'viewAllEvents\'',
					'items': {
						'type': 'string',
						'enum': [
							'modifyAcl',
							'viewAllEvents'
						],
						'enumDescriptions': [
							'',
							''
						]
					}
				},
				'role': {
					'type': 'string',
					'description': 'Access role granted to this scope.',
					'enum': [
						'manager',
						'owner',
						'robot',
						'user',
						'viewer'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						''
					],
					'annotations': {
						'required': [
							'clouddevices.aclEntries.insert'
						]
					}
				},
				'scopeId': {
					'type': 'string',
					'description': 'Email address if scope type is user or group, domain name if scope type is a domain.',
					'annotations': {
						'required': [
							'clouddevices.aclEntries.insert'
						]
					}
				},
				'scopeMembership': {
					'type': 'string',
					'description': 'Type of membership the user has in the scope.',
					'enum': [
						'delegator',
						'manager',
						'member',
						'none'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						''
					]
				},
				'scopeName': {
					'type': 'string',
					'description': 'Displayable scope name.'
				},
				'scopePhotoUrl': {
					'type': 'string',
					'description': 'URL of this scope displayable photo.'
				},
				'scopeType': {
					'type': 'string',
					'description': 'Type of the access scope.',
					'enum': [
						'application',
						'domain',
						'group',
						'public',
						'user'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						''
					]
				}
			}
		},
		'Application': {
			'id': 'Application',
			'type': 'object',
			'description': 'Contains information about a recommended application for a device model.',
			'properties': {
				'description': {
					'type': 'string',
					'description': 'User readable application description.'
				},
				'iconUrl': {
					'type': 'string',
					'description': 'Application icon URL.'
				},
				'id': {
					'type': 'string',
					'description': 'Unique application ID.'
				},
				'name': {
					'type': 'string',
					'description': 'User readable application name.'
				},
				'price': {
					'type': 'number',
					'description': 'Price of the application.',
					'format': 'double'
				},
				'publisherName': {
					'type': 'string',
					'description': 'User readable publisher name.'
				},
				'type': {
					'type': 'string',
					'description': 'Application type.',
					'enum': [
						'android',
						'chrome',
						'web'
					],
					'enumDescriptions': [
						'',
						'',
						''
					]
				},
				'url': {
					'type': 'string',
					'description': 'Application install URL.'
				}
			}
		},
		'AuthorizedApp': {
			'id': 'AuthorizedApp',
			'type': 'object',
			'properties': {
				'androidApps': {
					'type': 'array',
					'description': 'Android apps authorized under this project ID.',
					'items': {
						'type': 'object',
						'properties': {
							'certificate_hash': {
								'type': 'string',
								'description': 'Android certificate hash.'
							},
							'package_name': {
								'type': 'string',
								'description': 'Android package name.'
							}
						}
					}
				},
				'displayName': {
					'type': 'string',
					'description': 'The display name of the app.'
				},
				'iconUrl': {
					'type': 'string',
					'description': 'An icon for the app.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#authorizedApp\'.',
					'default': 'clouddevices#authorizedApp'
				},
				'projectId': {
					'type': 'string',
					'description': 'Project ID.'
				}
			}
		},
		'AuthorizedAppsCreateAppAuthenticationTokenResponse': {
			'id': 'AuthorizedAppsCreateAppAuthenticationTokenResponse',
			'type': 'object',
			'description': 'Generate a token used to authenticate an authorized app.',
			'properties': {
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#authorizedAppsCreateAppAuthenticationTokenResponse\'.',
					'default': 'clouddevices#authorizedAppsCreateAppAuthenticationTokenResponse'
				},
				'token': {
					'type': 'string',
					'description': 'Generated authentication token for an authorized app.'
				}
			}
		},
		'AuthorizedAppsListResponse': {
			'id': 'AuthorizedAppsListResponse',
			'type': 'object',
			'description': 'List of authorized apps.',
			'properties': {
				'authorizedApps': {
					'type': 'array',
					'description': 'The list of authorized apps.',
					'items': {
						'$ref': 'AuthorizedApp'
					}
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#authorizedAppsListResponse\'.',
					'default': 'clouddevices#authorizedAppsListResponse'
				}
			}
		},
		'Command': {
			'id': 'Command',
			'type': 'object',
			'externalTypeName': 'clouddevices.Command',
			'properties': {
				'blobParameters': {
					'$ref': 'JsonObject',
					'description': 'Blob parameters list.'
				},
				'blobResults': {
					'$ref': 'JsonObject',
					'description': 'Blob results list.'
				},
				'component': {
					'type': 'string',
					'description': 'Component name paths separated by ' / '.'
				},
				'creationTimeMs': {
					'type': 'string',
					'description': 'Timestamp since epoch of a creation of a command.',
					'format': 'int64'
				},
				'creatorEmail': {
					'type': 'string',
					'description': 'User that created the command (not applicable if the user is deleted).'
				},
				'deviceId': {
					'type': 'string',
					'description': 'Device ID that this command belongs to.',
					'annotations': {
						'required': [
							'clouddevices.commands.insert'
						]
					}
				},
				'error': {
					'type': 'object',
					'description': 'Error descriptor.',
					'properties': {
						'arguments': {
							'type': 'array',
							'description': 'Positional error arguments used for error message formatting.',
							'items': {
								'type': 'string'
							}
						},
						'code': {
							'type': 'string',
							'description': 'Error code.'
						},
						'message': {
							'type': 'string',
							'description': 'User-visible error message populated by the cloud based on command name and error code.'
						}
					}
				},
				'expirationTimeMs': {
					'type': 'string',
					'description': 'Timestamp since epoch of command expiration.',
					'format': 'int64'
				},
				'expirationTimeoutMs': {
					'type': 'string',
					'description': 'Expiration timeout for the command since its creation, 10 seconds min, 30 days max.',
					'format': 'int64'
				},
				'id': {
					'type': 'string',
					'description': 'Unique command ID.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#command\'.',
					'default': 'clouddevices#command'
				},
				'name': {
					'type': 'string',
					'description': 'Full command name, including trait.',
					'annotations': {
						'required': [
							'clouddevices.commands.insert'
						]
					}
				},
				'parameters': {
					'$ref': 'JsonObject',
					'description': 'Parameters list.'
				},
				'progress': {
					'$ref': 'JsonObject',
					'description': 'Command-specific progress descriptor.'
				},
				'results': {
					'$ref': 'JsonObject',
					'description': 'Results list.'
				},
				'state': {
					'type': 'string',
					'description': 'Current command state.',
					'enum': [
						'aborted',
						'cancelled',
						'done',
						'error',
						'expired',
						'inProgress',
						'queued'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						'',
						'',
						''
					]
				},
				'userAction': {
					'type': 'string',
					'description': 'Pending command state that is not acknowledged by the device yet.'
				}
			}
		},
		'CommandsListResponse': {
			'id': 'CommandsListResponse',
			'type': 'object',
			'description': 'List of commands.',
			'externalTypeName': 'clouddevices.api.CommandsListResponse',
			'properties': {
				'commands': {
					'type': 'array',
					'description': 'The actual list of commands.',
					'items': {
						'$ref': 'Command'
					}
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#commandsListResponse\'.',
					'default': 'clouddevices#commandsListResponse'
				},
				'nextPageToken': {
					'type': 'string',
					'description': 'Token for the next page of commands.'
				},
				'totalResults': {
					'type': 'integer',
					'description': 'The total number of commands for the query. The number of items in a response may be smaller due to paging.',
					'format': 'int32'
				}
			}
		},
		'CommandsQueueResponse': {
			'id': 'CommandsQueueResponse',
			'type': 'object',
			'externalTypeName': 'clouddevices.api.CommandsQueueResponse',
			'properties': {
				'commands': {
					'type': 'array',
					'description': 'Commands to be executed.',
					'items': {
						'$ref': 'Command'
					}
				}
			}
		},
		'Device': {
			'id': 'Device',
			'type': 'object',
			'externalTypeName': 'clouddevices.Device',
			'properties': {
				'certFingerprint': {
					'type': 'string',
					'description': 'The HTTPS certificate fingerprint used to secure communication with device..'
				},
				'channel': {
					'type': 'object',
					'description': 'Device notification channel description.',
					'properties': {
						'connectionStatusHint': {
							'type': 'string',
							'description': 'Connection status hint, set by parent device.',
							'enum': [
								'offline',
								'online',
								'unknown'
							],
							'enumDescriptions': [
								'',
								'',
								''
							]
						},
						'gcmRegistrationId': {
							'type': 'string',
							'description': 'GCM registration ID. Required if device supports GCM delivery channel.'
						},
						'gcmSenderId': {
							'type': 'string',
							'description': 'GCM sender ID. For Chrome apps must be the same as sender ID during registration, usually API project ID.'
						},
						'parentId': {
							'type': 'string',
							'description': 'Parent device ID (aggregator) if it exists.'
						},
						'supportedType': {
							'type': 'string',
							'description': 'Channel type supported by device.',
							'enum': [
								'dev_null',
								'gcm',
								'gcp',
								'parent',
								'pull',
								'xmpp'
							],
							'enumDescriptions': [
								'',
								'',
								'',
								'',
								'',
								''
							],
							'annotations': {
								'required': [
									'clouddevices.registrationTickets.insert'
								]
							}
						}
					},
					'annotations': {
						'required': [
							'clouddevices.devices.insert'
						]
					}
				},
				'commandDefs': {
					'type': 'object',
					'description': 'Description of commands supported by the device. This field is writable only by devices.',
					'additionalProperties': {
						'$ref': 'PackageDef'
					},
					'annotations': {
						'required': [
							'clouddevices.devices.insert'
						]
					}
				},
				'components': {
					'$ref': 'JsonObject',
					'description': 'Hierarchical componenet-based modeling of the device.'
				},
				'connectionStatus': {
					'type': 'string',
					'description': 'Device connection status.'
				},
				'creationTimeMs': {
					'type': 'string',
					'description': 'Timestamp of creation of this device in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'description': {
					'type': 'string',
					'description': 'User readable description of this device.'
				},
				'deviceKind': {
					'type': 'string',
					'description': 'Device kind. Deprecated, provide \'modelManifestId\' instead.',
					'enum': [
						'accessPoint',
						'aggregator',
						'camera',
						'developmentBoard',
						'lock',
						'printer',
						'scanner',
						'speaker',
						'storage',
						'toy',
						'vendor',
						'video'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						''
					]
				},
				'deviceLocalId': {
					'type': 'string',
					'description': 'The ID of the device for use on the local network.'
				},
				'id': {
					'type': 'string',
					'description': 'Unique device ID.'
				},
				'invitations': {
					'type': 'array',
					'description': 'List of pending invitations for the currently logged-in user.',
					'items': {
						'$ref': 'Invitation'
					}
				},
				'isEventRecordingDisabled': {
					'type': 'boolean',
					'description': 'Indicates whether event recording is enabled or disabled for this device.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#device\'.',
					'default': 'clouddevices#device'
				},
				'lastSeenTimeMs': {
					'type': 'string',
					'description': 'Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.',
					'format': 'int64'
				},
				'lastUpdateTimeMs': {
					'type': 'string',
					'description': 'Timestamp of the last device update in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'lastUseTimeMs': {
					'type': 'string',
					'description': 'Timestamp of the last device usage in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'location': {
					'type': 'string',
					'description': 'User readable location of the device (name of the room, office number, building/floor, etc).'
				},
				'modelManifest': {
					'type': 'object',
					'description': 'Device model information provided by the model manifest of this device.',
					'properties': {
						'modelName': {
							'type': 'string',
							'description': 'Device model name.'
						},
						'oemName': {
							'type': 'string',
							'description': 'Name of device model manufacturer.'
						}
					}
				},
				'modelManifestId': {
					'type': 'string',
					'description': 'Model manifest ID of this device.'
				},
				'name': {
					'type': 'string',
					'description': 'Name of this device provided by the manufacturer.'
				},
				'owner': {
					'type': 'string',
					'description': 'E-mail address of the device owner.'
				},
				'personalizedInfo': {
					'type': 'object',
					'description': 'Personalized device information for currently logged-in user.',
					'properties': {
						'lastUseTimeMs': {
							'type': 'string',
							'description': 'Timestamp of the last device usage by the user in milliseconds since epoch UTC.',
							'format': 'int64'
						},
						'location': {
							'type': 'string',
							'description': 'Personalized device location.'
						},
						'maxRole': {
							'type': 'string',
							'description': 'The maximum role on the device.'
						},
						'name': {
							'type': 'string',
							'description': 'Personalized device display name.'
						}
					}
				},
				'serialNumber': {
					'type': 'string',
					'description': 'Serial number of a device provided by its manufacturer.',
					'annotations': {
						'required': [
							'clouddevices.devices.insert'
						]
					}
				},
				'state': {
					'$ref': 'JsonObject',
					'description': 'Device state. This field is writable only by devices.'
				},
				'stateDefs': {
					'type': 'object',
					'description': 'Description of the device state. This field is writable only by devices.',
					'additionalProperties': {
						'$ref': 'StateDef'
					}
				},
				'tags': {
					'type': 'array',
					'description': 'Custom free-form manufacturer tags.',
					'items': {
						'type': 'string'
					}
				},
				'traits': {
					'$ref': 'JsonObject',
					'description': 'Traits defined for the device.'
				},
				'uiDeviceKind': {
					'type': 'string',
					'description': 'Device kind from the model manifest used in UI applications.'
				}
			}
		},
		'DeviceStatePatchesStatePatch': {
			'id': 'DeviceStatePatchesStatePatch',
			'type': 'object',
			'description': 'Device state patch with corresponding timestamp.',
			'properties': {
				'component': {
					'type': 'string',
					'description': 'Component name paths separated by ' / '.'
				},
				'patch': {
					'$ref': 'JsonObject',
					'description': 'State patch.'
				},
				'timeMs': {
					'type': 'string',
					'description': 'Timestamp of a change. Local time, UNIX timestamp or time since last boot can be used.',
					'format': 'int64'
				}
			}
		},
		'DevicesCreateLocalAuthTokensRequest': {
			'id': 'DevicesCreateLocalAuthTokensRequest',
			'type': 'object',
			'properties': {
				'deviceIds': {
					'type': 'array',
					'description': 'Device IDs.',
					'items': {
						'type': 'string'
					}
				}
			}
		},
		'DevicesCreateLocalAuthTokensResponse': {
			'id': 'DevicesCreateLocalAuthTokensResponse',
			'type': 'object',
			'properties': {
				'mintedLocalAuthTokens': {
					'type': 'array',
					'description': 'Minted device and client tokens.',
					'items': {
						'$ref': 'MintedLocalAuthInfo'
					}
				}
			}
		},
		'DevicesListResponse': {
			'id': 'DevicesListResponse',
			'type': 'object',
			'description': 'List of devices.',
			'externalTypeName': 'clouddevices.api.DevicesListResponse',
			'properties': {
				'devices': {
					'type': 'array',
					'description': 'The actual list of devices.',
					'items': {
						'$ref': 'Device'
					}
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#devicesListResponse\'.',
					'default': 'clouddevices#devicesListResponse'
				},
				'nextPageToken': {
					'type': 'string',
					'description': 'Token corresponding to the next page of devices.'
				},
				'totalResults': {
					'type': 'integer',
					'description': 'The total number of devices for the query. The number of items in a response may be smaller due to paging.',
					'format': 'int32'
				}
			}
		},
		'DevicesPatchStateRequest': {
			'id': 'DevicesPatchStateRequest',
			'type': 'object',
			'properties': {
				'patches': {
					'type': 'array',
					'description': 'The list of state patches with corresponding timestamps.',
					'items': {
						'$ref': 'DeviceStatePatchesStatePatch'
					}
				},
				'requestTimeMs': {
					'type': 'string',
					'description': 'Timestamp of a request. Local time, UNIX timestamp or time since last boot can be used.',
					'format': 'int64'
				}
			}
		},
		'DevicesPatchStateResponse': {
			'id': 'DevicesPatchStateResponse',
			'type': 'object',
			'properties': {
				'state': {
					'$ref': 'JsonObject',
					'description': 'Updated device state.'
				}
			}
		},
		'DevicesUpsertLocalAuthInfoRequest': {
			'id': 'DevicesUpsertLocalAuthInfoRequest',
			'type': 'object',
			'properties': {
				'localAuthInfo': {
					'$ref': 'LocalAuthInfo',
					'description': 'The local auth info of the device.'
				}
			}
		},
		'DevicesUpsertLocalAuthInfoResponse': {
			'id': 'DevicesUpsertLocalAuthInfoResponse',
			'type': 'object',
			'properties': {
				'localAuthInfo': {
					'$ref': 'LocalAuthInfo',
					'description': 'The non-secret local auth info.'
				}
			}
		},
		'Event': {
			'id': 'Event',
			'type': 'object',
			'externalTypeName': 'clouddevices.Event',
			'properties': {
				'commandPatch': {
					'type': 'object',
					'description': 'Command-related changes (if applicable).',
					'properties': {
						'commandId': {
							'type': 'string',
							'description': 'ID of the affected command.'
						},
						'state': {
							'type': 'string',
							'description': 'New command state.'
						}
					}
				},
				'connectionStatus': {
					'type': 'string',
					'description': 'New device connection state (if connectivity change event).'
				},
				'deviceId': {
					'type': 'string',
					'description': 'The device that was affected by this event.'
				},
				'id': {
					'type': 'string',
					'description': 'ID of the event.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#event\'.',
					'default': 'clouddevices#event'
				},
				'statePatch': {
					'$ref': 'JsonObject',
					'description': 'The device state patch (if applicable).'
				},
				'timeMs': {
					'type': 'string',
					'description': 'Time the event was generated in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'type': {
					'type': 'string',
					'description': 'Type of the event.',
					'enum': [
						'commandCancelled',
						'commandCreated',
						'commandExpired',
						'commandUpdated',
						'deviceAclUpdated',
						'deviceConnectivityChange',
						'deviceCreated',
						'deviceDeleted',
						'deviceUpdated',
						'eventsRecordingDisabled',
						'eventsRecordingEnabled'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						''
					]
				},
				'userEmail': {
					'type': 'string',
					'description': 'User that caused the event (if applicable).'
				}
			}
		},
		'EventsListResponse': {
			'id': 'EventsListResponse',
			'type': 'object',
			'description': 'List of events.',
			'externalTypeName': 'clouddevices.api.EventsListResponse',
			'properties': {
				'events': {
					'type': 'array',
					'description': 'The actual list of events in reverse chronological order.',
					'items': {
						'$ref': 'Event'
					}
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#eventsListResponse\'.',
					'default': 'clouddevices#eventsListResponse'
				},
				'nextPageToken': {
					'type': 'string',
					'description': 'Token for the next page of events.'
				},
				'totalResults': {
					'type': 'integer',
					'description': 'The total number of events for the query. The number of items in a response may be smaller due to paging.',
					'format': 'int32'
				}
			}
		},
		'EventsRecordDeviceEventsRequest': {
			'id': 'EventsRecordDeviceEventsRequest',
			'type': 'object',
			'properties': {
				'deviceId': {
					'type': 'string',
					'description': 'Device ID.'
				},
				'recordDeviceEvents': {
					'type': 'boolean',
					'description': 'Flag to indicate whether recording should be enabled or disabled.'
				}
			}
		},
		'Invitation': {
			'id': 'Invitation',
			'type': 'object',
			'externalTypeName': 'clouddevices.Invitation',
			'properties': {
				'aclEntry': {
					'$ref': 'AclEntry',
					'description': 'ACL entry associated with this invitation.'
				},
				'creatorEmail': {
					'type': 'string',
					'description': 'Email of a user who created this invitation.'
				}
			}
		},
		'JsonObject': {
			'id': 'JsonObject',
			'type': 'object',
			'description': 'JSON object value.',
			'additionalProperties': {
				'$ref': 'JsonValue'
			}
		},
		'JsonValue': {
			'id': 'JsonValue',
			'type': 'any',
			'description': 'JSON value -- union over JSON value types.'
		},
		'LocalAccessEntry': {
			'id': 'LocalAccessEntry',
			'type': 'object',
			'properties': {
				'gaiaId': {
					'type': 'string',
					'description': 'Gaia id of the user that this access info is associated with.',
					'format': 'int64'
				},
				'isApp': {
					'type': 'boolean',
					'description': 'Whether this belongs to a delegated app or user.'
				},
				'localAccessRole': {
					'type': 'string',
					'description': 'Access role of the user.',
					'enum': [
						'manager',
						'owner',
						'robot',
						'user',
						'viewer'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						''
					]
				},
				'projectId': {
					'type': 'string',
					'description': 'Project id of the app that this access info is associated with.',
					'format': 'int64'
				}
			}
		},
		'LocalAccessInfo': {
			'id': 'LocalAccessInfo',
			'type': 'object',
			'properties': {
				'localAccessEntry': {
					'$ref': 'LocalAccessEntry',
					'description': 'A snapshot of the access entry used at the time of minting.'
				},
				'localAuthTokenMintTimeMs': {
					'type': 'string',
					'description': 'Time in milliseconds since unix epoch of when the local auth token was minted.',
					'format': 'int64'
				},
				'localAuthTokenTimeLeftMs': {
					'type': 'string',
					'description': 'Relative time left of token after API call.',
					'format': 'int64'
				},
				'localAuthTokenTtlTimeMs': {
					'type': 'string',
					'description': 'Time in milliseconds of hold long the token is valid after minting.',
					'format': 'int64'
				}
			}
		},
		'LocalAuthInfo': {
			'id': 'LocalAuthInfo',
			'type': 'object',
			'properties': {
				'certFingerprint': {
					'type': 'string'
				},
				'clientToken': {
					'type': 'string'
				},
				'deviceToken': {
					'type': 'string'
				},
				'localId': {
					'type': 'string'
				}
			}
		},
		'MintedLocalAuthInfo': {
			'id': 'MintedLocalAuthInfo',
			'type': 'object',
			'properties': {
				'clientToken': {
					'type': 'string'
				},
				'deviceId': {
					'type': 'string'
				},
				'deviceToken': {
					'type': 'string'
				},
				'localAccessInfo': {
					'$ref': 'LocalAccessInfo'
				},
				'retryAfter': {
					'type': 'string',
					'format': 'int64'
				}
			}
		},
		'ModelManifest': {
			'id': 'ModelManifest',
			'type': 'object',
			'description': 'Model manifest info.',
			'externalTypeName': 'clouddevices.ModelManifest',
			'properties': {
				'allowedChildModelManifestIds': {
					'type': 'array',
					'description': 'For gateways, a list of device ids that are allowed to connect to it.',
					'items': {
						'type': 'string'
					}
				},
				'applications': {
					'type': 'array',
					'description': 'List of applications recommended to use with a device model.',
					'items': {
						'$ref': 'Application'
					}
				},
				'confirmationImageUrl': {
					'type': 'string',
					'description': 'URL of image showing a confirmation button.'
				},
				'deviceImageUrl': {
					'type': 'string',
					'description': 'URL of device image.'
				},
				'deviceKind': {
					'type': 'string',
					'description': 'Device kind, see \'deviceKind\' field of the Device resource.',
					'enum': [
						'accessPoint',
						'aggregator',
						'camera',
						'developmentBoard',
						'lock',
						'printer',
						'scanner',
						'speaker',
						'storage',
						'toy',
						'vendor',
						'video'
					],
					'enumDescriptions': [
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						'',
						''
					]
				},
				'id': {
					'type': 'string',
					'description': 'Unique model manifest ID.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#modelManifest\'.',
					'default': 'clouddevices#modelManifest'
				},
				'modelDescription': {
					'type': 'string',
					'description': 'User readable device model description.'
				},
				'modelName': {
					'type': 'string',
					'description': 'User readable device model name.'
				},
				'oemName': {
					'type': 'string',
					'description': 'User readable name of device model manufacturer.'
				},
				'supportPageUrl': {
					'type': 'string',
					'description': 'URL of device support page.'
				}
			}
		},
		'ModelManifestsListResponse': {
			'id': 'ModelManifestsListResponse',
			'type': 'object',
			'description': 'List of model manifests.',
			'externalTypeName': 'clouddevices.api.ModelManifestsListResponse',
			'properties': {
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#modelManifestsListResponse\'.',
					'default': 'clouddevices#modelManifestsListResponse'
				},
				'modelManifests': {
					'type': 'array',
					'description': 'The actual list of model manifests.',
					'items': {
						'$ref': 'ModelManifest'
					}
				},
				'nextPageToken': {
					'type': 'string',
					'description': 'Token corresponding to the next page of model manifests.'
				},
				'totalResults': {
					'type': 'integer',
					'description': 'The total number of model manifests for the query. The number of items in a response may be smaller due to paging.',
					'format': 'int32'
				}
			}
		},
		'ModelManifestsValidateCommandDefsRequest': {
			'id': 'ModelManifestsValidateCommandDefsRequest',
			'type': 'object',
			'externalTypeName': 'clouddevices.api.ModelManifestsValidateCommandDefsRequest',
			'properties': {
				'commandDefs': {
					'type': 'object',
					'description': 'Description of commands.',
					'additionalProperties': {
						'$ref': 'PackageDef'
					}
				}
			}
		},
		'ModelManifestsValidateCommandDefsResponse': {
			'id': 'ModelManifestsValidateCommandDefsResponse',
			'type': 'object',
			'properties': {
				'validationErrors': {
					'type': 'array',
					'description': 'Validation errors in command definitions.',
					'items': {
						'type': 'string'
					}
				}
			}
		},
		'ModelManifestsValidateDeviceStateRequest': {
			'id': 'ModelManifestsValidateDeviceStateRequest',
			'type': 'object',
			'properties': {
				'state': {
					'$ref': 'JsonObject',
					'description': 'Device state object.'
				}
			}
		},
		'ModelManifestsValidateDeviceStateResponse': {
			'id': 'ModelManifestsValidateDeviceStateResponse',
			'type': 'object',
			'properties': {
				'validationErrors': {
					'type': 'array',
					'description': 'Validation errors in device state.',
					'items': {
						'type': 'string'
					}
				}
			}
		},
		'PackageDef': {
			'id': 'PackageDef',
			'type': 'object',
			'externalTypeName': 'clouddevices.PackageDef',
			'additionalProperties': {
				'type': 'object',
				'properties': {
					'displayName': {
						'type': 'string',
						'description': 'Display name of the command.'
					},
					'kind': {
						'type': 'string',
						'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#commandDef\'.',
						'default': 'clouddevices#commandDef'
					},
					'minimalRole': {
						'type': 'string',
						'description': 'Minimal role required to execute command.',
						'enum': [
							'manager',
							'owner',
							'user',
							'viewer'
						],
						'enumDescriptions': [
							'',
							'',
							'',
							''
						]
					},
					'parameters': {
						'type': 'object',
						'description': 'Parameters of the command.',
						'additionalProperties': {
							'$ref': 'JsonObject'
						}
					}
				}
			}
		},
		'PersonalizedInfo': {
			'id': 'PersonalizedInfo',
			'type': 'object',
			'externalTypeName': 'clouddevices.PersonalizedInfo',
			'properties': {
				'id': {
					'type': 'string',
					'description': 'Unique personalizedInfo ID. Value: the fixed string \'me\'.',
					'default': 'me'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#personalizedInfo\'.',
					'default': 'clouddevices#personalizedInfo'
				},
				'lastUseTimeMs': {
					'type': 'string',
					'description': 'Timestamp of the last device usage by the user in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'location': {
					'type': 'string',
					'description': 'Personalized device location.'
				},
				'name': {
					'type': 'string',
					'description': 'Personalized device display name.'
				}
			}
		},
		'RegistrationTicket': {
			'id': 'RegistrationTicket',
			'type': 'object',
			'externalTypeName': 'clouddevices.RegistrationTicket',
			'properties': {
				'creationTimeMs': {
					'type': 'string',
					'description': 'Creation timestamp of the registration ticket in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'deviceDraft': {
					'$ref': 'Device',
					'description': 'Draft of the device being registered.'
				},
				'deviceId': {
					'type': 'string',
					'description': 'ID that device will have after registration is successfully finished.'
				},
				'errorCode': {
					'type': 'string',
					'description': 'Error code. Set only on device registration failures.'
				},
				'expirationTimeMs': {
					'type': 'string',
					'description': 'Expiration timestamp of the registration ticket in milliseconds since epoch UTC.',
					'format': 'int64'
				},
				'id': {
					'type': 'string',
					'description': 'Registration ticket ID.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#registrationTicket\'.',
					'default': 'clouddevices#registrationTicket'
				},
				'oauthClientId': {
					'type': 'string',
					'description': 'OAuth 2.0 client ID of the device.'
				},
				'parentId': {
					'type': 'string',
					'description': 'Parent device ID (aggregator) if it exists.'
				},
				'robotAccountAuthorizationCode': {
					'type': 'string',
					'description': 'Authorization code that can be exchanged to a refresh token.'
				},
				'robotAccountEmail': {
					'type': 'string',
					'description': 'E-mail address of robot account assigned to the registered device.'
				},
				'userEmail': {
					'type': 'string',
					'description': 'Email address of the owner.'
				}
			}
		},
		'StateDef': {
			'id': 'StateDef',
			'type': 'object',
			'externalTypeName': 'clouddevices.StatePackageDef',
			'additionalProperties': {
				'type': 'object',
				'properties': {
					'kind': {
						'type': 'string',
						'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#stateDef\'.',
						'default': 'clouddevices#stateDef'
					},
					'minimalRole': {
						'type': 'string',
						'description': 'Minimal role required to view state.',
						'enum': [
							'manager',
							'owner',
							'user',
							'viewer'
						],
						'enumDescriptions': [
							'',
							'',
							'',
							''
						]
					},
					'name': {
						'type': 'string',
						'description': 'Name of the state field.'
					}
				}
			}
		},
		'SubscriptionData': {
			'id': 'SubscriptionData',
			'type': 'object',
			'description': 'Subscription template.',
			'externalTypeName': 'clouddevices.SubscriptionData',
			'properties': {
				'expirationTimeMs': {
					'type': 'string',
					'description': 'Timestamp in milliseconds since epoch when the subscription expires and new notifications stop being sent.',
					'format': 'int64'
				},
				'filters': {
					'type': 'array',
					'description': 'Subscription event filter.\n\nAcceptable values are:  \n- \'myDevices\' \n- \'myCommands\'',
					'items': {
						'type': 'string',
						'enum': [
							'myCommands',
							'myDevices'
						],
						'enumDescriptions': [
							'',
							''
						]
					}
				},
				'gcmRegistrationId': {
					'type': 'string',
					'description': 'GCM registration ID.'
				},
				'gcmSenderId': {
					'type': 'string',
					'description': 'For Chrome apps must be the same as sender ID during registration, usually API project ID.'
				},
				'kind': {
					'type': 'string',
					'description': 'Identifies what kind of resource this is. Value: the fixed string \'clouddevices#subscription\'.',
					'default': 'clouddevices#subscription'
				}
			}
		}
	}
};
