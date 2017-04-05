# swagger_client.DevicesApi

All URIs are relative to *https://localhost/weaviate/v1-alpha*

Method | HTTP request | Description
------------- | ------------- | -------------
[**weave_devices_add_label**](DevicesApi.md#weave_devices_add_label) | **POST** /devices/{deviceId}/addLabel | 
[**weave_devices_add_nickname**](DevicesApi.md#weave_devices_add_nickname) | **POST** /devices/{deviceId}/addNickname | 
[**weave_devices_delete**](DevicesApi.md#weave_devices_delete) | **DELETE** /devices/{deviceId} | 
[**weave_devices_get**](DevicesApi.md#weave_devices_get) | **GET** /devices/{deviceId} | 
[**weave_devices_insert**](DevicesApi.md#weave_devices_insert) | **POST** /devices | 
[**weave_devices_list**](DevicesApi.md#weave_devices_list) | **GET** /devices | 
[**weave_devices_patch**](DevicesApi.md#weave_devices_patch) | **PATCH** /devices/{deviceId} | 
[**weave_devices_patch_state**](DevicesApi.md#weave_devices_patch_state) | **POST** /devices/{deviceId}/patchState | 
[**weave_devices_remove_label**](DevicesApi.md#weave_devices_remove_label) | **POST** /devices/{deviceId}/removeLabel | 
[**weave_devices_remove_nickname**](DevicesApi.md#weave_devices_remove_nickname) | **POST** /devices/{deviceId}/removeNickname | 
[**weave_devices_update**](DevicesApi.md#weave_devices_update) | **PUT** /devices/{deviceId} | 
[**weave_devices_update_parent**](DevicesApi.md#weave_devices_update_parent) | **POST** /devices/{deviceId}/updateParent | 


# **weave_devices_add_label**
> weave_devices_add_label(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Adds a label to the device.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Device id.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.DevicesAddLabelRequest() # DevicesAddLabelRequest |  (optional)

try: 
    api_instance.weave_devices_add_label(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_add_label: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Device id. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**DevicesAddLabelRequest**](DevicesAddLabelRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_add_nickname**
> DevicesAddNicknameResponse weave_devices_add_nickname(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Adds a nickname to the device.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Device id.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.DevicesAddNicknameRequest() # DevicesAddNicknameRequest |  (optional)

try: 
    api_response = api_instance.weave_devices_add_nickname(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_add_nickname: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Device id. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**DevicesAddNicknameRequest**](DevicesAddNicknameRequest.md)|  | [optional] 

### Return type

[**DevicesAddNicknameResponse**](DevicesAddNicknameResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_delete**
> weave_devices_delete(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)



Deletes a device from the system.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Unique ID of the device.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)

try: 
    api_instance.weave_devices_delete(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_delete: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Unique ID of the device. | 
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

# **weave_devices_get**
> Device weave_devices_get(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, projection=projection)



Returns a particular device data.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Unique ID of the device.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
projection = 'projection_example' # str | Projection controls which fields of the Device resource are returned. (optional)

try: 
    api_response = api_instance.weave_devices_get(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, projection=projection)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_get: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Unique ID of the device. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **projection** | **str**| Projection controls which fields of the Device resource are returned. | [optional] 

### Return type

[**Device**](Device.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_insert**
> Device weave_devices_insert(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, adapter_activation_id=adapter_activation_id, hl=hl, body=body)



Registers a new device. This method may be used only by aggregator devices or adapters.

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
api_instance = swagger_client.DevicesApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
adapter_activation_id = 'adapter_activation_id_example' # str | ID of the adapter activation that this device belongs to, if any. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.Device() # Device |  (optional)

try: 
    api_response = api_instance.weave_devices_insert(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, adapter_activation_id=adapter_activation_id, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_insert: %s\n" % e)
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
 **adapter_activation_id** | **str**| ID of the adapter activation that this device belongs to, if any. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**Device**](Device.md)|  | [optional] 

### Return type

[**Device**](Device.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_list**
> DevicesListResponse weave_devices_list(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, description_substring=description_substring, device_kind=device_kind, display_name_substring=display_name_substring, hl=hl, max_results=max_results, model_manifest_id=model_manifest_id, name_substring=name_substring, place_id=place_id, role=role, room_id=room_id, start_index=start_index, system_name_substring=system_name_substring, token=token)



Lists devices user has access to.

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
api_instance = swagger_client.DevicesApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
description_substring = 'description_substring_example' # str | Device description. (optional)
device_kind = 'device_kind_example' # str | Device kind. (optional)
display_name_substring = 'display_name_substring_example' # str | Device display name. Deprecated, use 'nameSubstring' instead. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
max_results = 56 # int |  (optional)
model_manifest_id = 'model_manifest_id_example' # str | Device model manifest. (optional)
name_substring = 'name_substring_example' # str | Device name. (optional)
place_id = 'place_id_example' # str | Device's place. (optional)
role = 'role_example' # str | Access role to the device. (optional)
room_id = 'room_id_example' # str | Device's room. (optional)
start_index = 56 # int |  (optional)
system_name_substring = 'system_name_substring_example' # str | Device system name. Deprecated, use 'nameSubstring' instead. (optional)
token = 'token_example' # str |  (optional)

try: 
    api_response = api_instance.weave_devices_list(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, description_substring=description_substring, device_kind=device_kind, display_name_substring=display_name_substring, hl=hl, max_results=max_results, model_manifest_id=model_manifest_id, name_substring=name_substring, place_id=place_id, role=role, room_id=room_id, start_index=start_index, system_name_substring=system_name_substring, token=token)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_list: %s\n" % e)
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
 **description_substring** | **str**| Device description. | [optional] 
 **device_kind** | **str**| Device kind. | [optional] 
 **display_name_substring** | **str**| Device display name. Deprecated, use &#39;nameSubstring&#39; instead. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **max_results** | **int**|  | [optional] 
 **model_manifest_id** | **str**| Device model manifest. | [optional] 
 **name_substring** | **str**| Device name. | [optional] 
 **place_id** | **str**| Device&#39;s place. | [optional] 
 **role** | **str**| Access role to the device. | [optional] 
 **room_id** | **str**| Device&#39;s room. | [optional] 
 **start_index** | **int**|  | [optional] 
 **system_name_substring** | **str**| Device system name. Deprecated, use &#39;nameSubstring&#39; instead. | [optional] 
 **token** | **str**|  | [optional] 

### Return type

[**DevicesListResponse**](DevicesListResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_patch**
> Device weave_devices_patch(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, last_update_time_ms=last_update_time_ms, body=body)



Updates a device data. This method supports patch semantics.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Unique ID of the device.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
last_update_time_ms = 'last_update_time_ms_example' # str | Previous last update time in device data. Optionally set this parameter to ensure an update call does not overwrite newer data. (optional)
body = swagger_client.Device() # Device |  (optional)

try: 
    api_response = api_instance.weave_devices_patch(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, last_update_time_ms=last_update_time_ms, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_patch: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Unique ID of the device. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **last_update_time_ms** | **str**| Previous last update time in device data. Optionally set this parameter to ensure an update call does not overwrite newer data. | [optional] 
 **body** | [**Device**](Device.md)|  | [optional] 

### Return type

[**Device**](Device.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_patch_state**
> DevicesPatchStateResponse weave_devices_patch_state(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Applies provided patches to the device state. This method may be used only by devices.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Device id.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.DevicesPatchStateRequest() # DevicesPatchStateRequest |  (optional)

try: 
    api_response = api_instance.weave_devices_patch_state(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_patch_state: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Device id. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**DevicesPatchStateRequest**](DevicesPatchStateRequest.md)|  | [optional] 

### Return type

[**DevicesPatchStateResponse**](DevicesPatchStateResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_remove_label**
> weave_devices_remove_label(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Removes a label of the device.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Device id.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.DevicesRemoveLabelRequest() # DevicesRemoveLabelRequest |  (optional)

try: 
    api_instance.weave_devices_remove_label(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_remove_label: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Device id. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**DevicesRemoveLabelRequest**](DevicesRemoveLabelRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_remove_nickname**
> DevicesRemoveNicknameResponse weave_devices_remove_nickname(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Removes a nickname of the device.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Device id.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.DevicesRemoveNicknameRequest() # DevicesRemoveNicknameRequest |  (optional)

try: 
    api_response = api_instance.weave_devices_remove_nickname(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_remove_nickname: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Device id. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **body** | [**DevicesRemoveNicknameRequest**](DevicesRemoveNicknameRequest.md)|  | [optional] 

### Return type

[**DevicesRemoveNicknameResponse**](DevicesRemoveNicknameResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_update**
> Device weave_devices_update(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, last_update_time_ms=last_update_time_ms, body=body)



Updates a device data.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Unique ID of the device.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
last_update_time_ms = 'last_update_time_ms_example' # str | Previous last update time in device data. Optionally set this parameter to ensure an update call does not overwrite newer data. (optional)
body = swagger_client.Device() # Device |  (optional)

try: 
    api_response = api_instance.weave_devices_update(device_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, last_update_time_ms=last_update_time_ms, body=body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_update: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Unique ID of the device. | 
 **alt** | **str**| Data format for the response. | [optional] [default to json]
 **fields** | **str**| Selector specifying which fields to include in a partial response. | [optional] 
 **key** | **str**| API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. | [optional] 
 **oauth_token** | **str**| OAuth 2.0 token for the current user. | [optional] 
 **pretty_print** | **bool**| Returns response with indentations and line breaks. | [optional] [default to true]
 **quota_user** | **str**| Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. | [optional] 
 **user_ip** | **str**| IP address of the site where the request originates. Use this if you want to enforce per-user limits. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **last_update_time_ms** | **str**| Previous last update time in device data. Optionally set this parameter to ensure an update call does not overwrite newer data. | [optional] 
 **body** | [**Device**](Device.md)|  | [optional] 

### Return type

[**Device**](Device.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_devices_update_parent**
> weave_devices_update_parent(device_id, parent_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)



Updates parent of the child device. Only managers can use this method.

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
api_instance = swagger_client.DevicesApi()
device_id = 'device_id_example' # str | Device ID.
parent_id = 'parent_id_example' # str | New parent device ID.
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)

try: 
    api_instance.weave_devices_update_parent(device_id, parent_id, alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl)
except ApiException as e:
    print("Exception when calling DevicesApi->weave_devices_update_parent: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **device_id** | **str**| Device ID. | 
 **parent_id** | **str**| New parent device ID. | 
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

