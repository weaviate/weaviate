'use strict';
module.exports = {
	'parameters': {
		'alt': {
			'type': 'string',
			'description': 'Data format for the response.',
			'default': 'json',
			'enum': [
				'json'
			],
			'enumDescriptions': [
				'Responses with Content-Type of application/json'
			],
			'location': 'query'
		},
		'fields': {
			'type': 'string',
			'description': 'Selector specifying which fields to include in a partial response.',
			'location': 'query'
		},
		'key': {
			'type': 'string',
			'description': 'API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token.',
			'location': 'query'
		},
		'oauth_token': {
			'type': 'string',
			'description': 'OAuth 2.0 token for the current user.',
			'location': 'query'
		},
		'prettyPrint': {
			'type': 'boolean',
			'description': 'Returns response with indentations and line breaks.',
			'default': 'true',
			'location': 'query'
		},
		'quotaUser': {
			'type': 'string',
			'description': 'Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided.',
			'location': 'query'
		},
		'userIp': {
			'type': 'string',
			'description': 'IP address of the site where the request originates. Use this if you want to enforce per-user limits.',
			'location': 'query'
		}
	}
};
