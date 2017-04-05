# swagger_client.AclEntriesApi

All URIs are relative to *https://localhost/weaviate/v1-alpha*

Method | HTTP request | Description
------------- | ------------- | -------------
[**weave_acl_entries_delete**](AclEntriesApi.md#weave_acl_entries_delete) | **DELETE** /devices/{deviceId}/aclEntries/{aclEntryId} | 
[**weave_acl_entries_get**](AclEntriesApi.md#weave_acl_entries_get) | **GET** /devices/{deviceId}/aclEntries/{aclEntryId} | 
[**weave_acl_entries_insert**](AclEntriesApi.md#weave_acl_entries_insert) | **POST** /devices/{deviceId}/aclEntries | 
[**weave_acl_entries_list**](AclEntriesApi.md#weave_acl_entries_list) | **GET** /devices/{deviceId}/aclEntries | 
[**weave_acl_entries_patch**](AclEntriesApi.md#weave_acl_entries_patch) | **PATCH** /devices/{deviceId}/aclEntries/{aclEntryId} | 
[**weave_acl_entries_update**](AclEntriesApi.md#weave_acl_entries_update) | **PUT** /devices/{deviceId}/aclEntries/{aclEntryId} | 


# **weave_acl_entries_delete**
> weave_acl_entries_delete(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)



Deletes an ACL entry.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# Configure API key authorization: apiKey
swagger_client.configuration.api_key['X-API-KEY'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# swagger_client.configuration.api_key_prefix['X-API-KEY'] = 'Bearer'

# create an instance of the API class
api_instance = swagger_client.AclEntriesApi()
device_id = 'device_id_example' # str | ID of the device to use.
acl_entry_id = 'acl_entry_id_example' # str | Unique ACL entry ID.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)

try: 
    api_instance.weave_acl_entries_delete(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)
except ApiException as e:
    print("Exception when calling AclEntriesApi->weave_acl_entries_delete: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| ID of the device to use. | 
 **acl_entry_id** | **str**| Unique ACL entry ID. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 

### Return type

void (empty response body)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_acl_entries_get**
> AclEntry weave_acl_entries_get(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)



Returns the requested ACL entry.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# Configure API key authorization: apiKey
swagger_client.configuration.api_key['X-API-KEY'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# swagger_client.configuration.api_key_prefix['X-API-KEY'] = 'Bearer'

# create an instance of the API class
api_instance = swagger_client.AclEntriesApi()
device_id = 'device_id_example' # str | ID of the device to use.
acl_entry_id = 'acl_entry_id_example' # str | Unique ACL entry ID.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)

try: 
    api_response = api_instance.weave_acl_entries_get(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling AclEntriesApi->weave_acl_entries_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| ID of the device to use. | 
 **acl_entry_id** | **str**| Unique ACL entry ID. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 

### Return type

[**AclEntry**](AclEntry.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_acl_entries_insert**
> AclEntry weave_acl_entries_insert(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Inserts a new ACL entry.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# Configure API key authorization: apiKey
swagger_client.configuration.api_key['X-API-KEY'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# swagger_client.configuration.api_key_prefix['X-API-KEY'] = 'Bearer'

# create an instance of the API class
api_instance = swagger_client.AclEntriesApi()
device_id = 'device_id_example' # str | ID of the device to use.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.AclEntry() # AclEntry |  (optional)

try: 
    api_response = api_instance.weave_acl_entries_insert(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling AclEntriesApi->weave_acl_entries_insert: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| ID of the device to use. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**AclEntry**](AclEntry.md)|  | [optional] 

### Return type

[**AclEntry**](AclEntry.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_acl_entries_list**
> AclEntriesListResponse weave_acl_entries_list(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, max_results=max_results, start_index=start_index, token=token)



Lists ACL entries.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# Configure API key authorization: apiKey
swagger_client.configuration.api_key['X-API-KEY'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# swagger_client.configuration.api_key_prefix['X-API-KEY'] = 'Bearer'

# create an instance of the API class
api_instance = swagger_client.AclEntriesApi()
device_id = 'device_id_example' # str | ID of the device to use.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
max_results = 56 # int |  (optional)
start_index = 56 # int |  (optional)
token = 'token_example' # str |  (optional)

try: 
    api_response = api_instance.weave_acl_entries_list(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, max_results=max_results, start_index=start_index, token=token)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling AclEntriesApi->weave_acl_entries_list: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| ID of the device to use. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **max_results** | **int**|  | [optional] 
 **start_index** | **int**|  | [optional] 
 **token** | **str**|  | [optional] 

### Return type

[**AclEntriesListResponse**](AclEntriesListResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_acl_entries_patch**
> AclEntry weave_acl_entries_patch(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Update an ACL entry. This method supports patch semantics.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# Configure API key authorization: apiKey
swagger_client.configuration.api_key['X-API-KEY'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# swagger_client.configuration.api_key_prefix['X-API-KEY'] = 'Bearer'

# create an instance of the API class
api_instance = swagger_client.AclEntriesApi()
device_id = 'device_id_example' # str | ID of the device to use.
acl_entry_id = 'acl_entry_id_example' # str | Unique ACL entry ID.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.AclEntry() # AclEntry |  (optional)

try: 
    api_response = api_instance.weave_acl_entries_patch(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling AclEntriesApi->weave_acl_entries_patch: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| ID of the device to use. | 
 **acl_entry_id** | **str**| Unique ACL entry ID. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**AclEntry**](AclEntry.md)|  | [optional] 

### Return type

[**AclEntry**](AclEntry.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_acl_entries_update**
> AclEntry weave_acl_entries_update(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Update an ACL entry.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# Configure API key authorization: apiKey
swagger_client.configuration.api_key['X-API-KEY'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# swagger_client.configuration.api_key_prefix['X-API-KEY'] = 'Bearer'

# create an instance of the API class
api_instance = swagger_client.AclEntriesApi()
device_id = 'device_id_example' # str | ID of the device to use.
acl_entry_id = 'acl_entry_id_example' # str | Unique ACL entry ID.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.AclEntry() # AclEntry |  (optional)

try: 
    api_response = api_instance.weave_acl_entries_update(device_id, acl_entry_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling AclEntriesApi->weave_acl_entries_update: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| ID of the device to use. | 
 **acl_entry_id** | **str**| Unique ACL entry ID. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**AclEntry**](AclEntry.md)|  | [optional] 

### Return type

[**AclEntry**](AclEntry.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

