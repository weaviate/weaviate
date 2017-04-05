# swagger_client.ModelManifestsApi

All URIs are relative to *https://localhost/weaviate/v1-alpha*

Method | HTTP request | Description
------------- | ------------- | -------------
[**weave_model_manifests_get**](ModelManifestsApi.md#weave_model_manifests_get) | **GET** /modelManifests/{modelManifestId} | 
[**weave_model_manifests_list**](ModelManifestsApi.md#weave_model_manifests_list) | **GET** /modelManifests | 
[**weave_model_manifests_validate_command_defs**](ModelManifestsApi.md#weave_model_manifests_validate_command_defs) | **POST** /modelManifests/validateCommandDefs | 
[**weave_model_manifests_validate_components**](ModelManifestsApi.md#weave_model_manifests_validate_components) | **POST** /modelManifests/validateComponents | 
[**weave_model_manifests_validate_device_state**](ModelManifestsApi.md#weave_model_manifests_validate_device_state) | **POST** /modelManifests/validateDeviceState | 


# **weave_model_manifests_get**
> ModelManifest weave_model_manifests_get(model_manifest_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)



Returns a particular model manifest.

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
api_instance = swagger_client.ModelManifestsApi()
model_manifest_id = 'model_manifest_id_example' # str | Unique ID of the model manifest.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)

try: 
    api_response = api_instance.weave_model_manifests_get(model_manifest_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ModelManifestsApi->weave_model_manifests_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **model_manifest_id** | **str**| Unique ID of the model manifest. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 

### Return type

[**ModelManifest**](ModelManifest.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_model_manifests_list**
> ModelManifestsListResponse weave_model_manifests_list(ids, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, max_results=max_results, start_index=start_index, token=token)



Lists all model manifests.

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
api_instance = swagger_client.ModelManifestsApi()
ids = ['ids_example'] # list[str] | Model manifest IDs to include in the result
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
    api_response = api_instance.weave_model_manifests_list(ids, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, max_results=max_results, start_index=start_index, token=token)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ModelManifestsApi->weave_model_manifests_list: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ids** | [**list[str]**](str.md)| Model manifest IDs to include in the result | 
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

[**ModelManifestsListResponse**](ModelManifestsListResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_model_manifests_validate_command_defs**
> ModelManifestsValidateCommandDefsResponse weave_model_manifests_validate_command_defs(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Validates given command definitions and returns errors.

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
api_instance = swagger_client.ModelManifestsApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.ModelManifestsValidateCommandDefsRequest() # ModelManifestsValidateCommandDefsRequest |  (optional)

try: 
    api_response = api_instance.weave_model_manifests_validate_command_defs(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ModelManifestsApi->weave_model_manifests_validate_command_defs: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**ModelManifestsValidateCommandDefsRequest**](ModelManifestsValidateCommandDefsRequest.md)|  | [optional] 

### Return type

[**ModelManifestsValidateCommandDefsResponse**](ModelManifestsValidateCommandDefsResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_model_manifests_validate_components**
> ModelManifestsValidateComponentsResponse weave_model_manifests_validate_components(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Validates given components definitions and returns errors.

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
api_instance = swagger_client.ModelManifestsApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.ModelManifestsValidateComponentsRequest() # ModelManifestsValidateComponentsRequest |  (optional)

try: 
    api_response = api_instance.weave_model_manifests_validate_components(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ModelManifestsApi->weave_model_manifests_validate_components: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**ModelManifestsValidateComponentsRequest**](ModelManifestsValidateComponentsRequest.md)|  | [optional] 

### Return type

[**ModelManifestsValidateComponentsResponse**](ModelManifestsValidateComponentsResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_model_manifests_validate_device_state**
> ModelManifestsValidateDeviceStateResponse weave_model_manifests_validate_device_state(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Validates given device state object and returns errors.

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
api_instance = swagger_client.ModelManifestsApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.ModelManifestsValidateDeviceStateRequest() # ModelManifestsValidateDeviceStateRequest |  (optional)

try: 
    api_response = api_instance.weave_model_manifests_validate_device_state(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ModelManifestsApi->weave_model_manifests_validate_device_state: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**ModelManifestsValidateDeviceStateRequest**](ModelManifestsValidateDeviceStateRequest.md)|  | [optional] 

### Return type

[**ModelManifestsValidateDeviceStateResponse**](ModelManifestsValidateDeviceStateResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

