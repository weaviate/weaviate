# swagger_client.EventsApi

All URIs are relative to *https://localhost/weaviate/v1-alpha*

Method | HTTP request | Description
------------- | ------------- | -------------
[**weave_events_delete_all**](EventsApi.md#weave_events_delete_all) | **POST** /events/deleteAll | 
[**weave_events_list**](EventsApi.md#weave_events_list) | **GET** /events | 
[**weave_events_record_device_events**](EventsApi.md#weave_events_record_device_events) | **POST** /events/recordDeviceEvents | 


# **weave_events_delete_all**
> weave_events_delete_all(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Deletes all events associated with a particular device. Leaves an event to indicate deletion happened.

### Example 
```python
from __future__ import print_statement
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = swagger_client.EventsApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.EventsDeleteAllRequest() # EventsDeleteAllRequest |  (optional)

try: 
    api_instance.weave_events_delete_all(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
except ApiException as e:
    print("Exception when calling EventsApi->weave_events_delete_all: %s\n" % e)
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
 **body** | [**EventsDeleteAllRequest**](EventsDeleteAllRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_events_list**
> EventsListResponse weave_events_list(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, command_id=command_id, device_id=device_id, end_time_ms=end_time_ms, hl=hl, max_results=max_results, start_index=start_index, start_time_ms=start_time_ms, token=token, type=type)



Lists events.

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
api_instance = swagger_client.EventsApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
command_id = ['command_id_example'] # list[str] | Affected command id. (optional)
device_id = ['device_id_example'] # list[str] | Sending or affected device id. (optional)
end_time_ms = 'end_time_ms_example' # str | End of time range in ms since epoch. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
max_results = 56 # int |  (optional)
start_index = 56 # int |  (optional)
start_time_ms = 'start_time_ms_example' # str | Start of time range in ms since epoch. (optional)
token = 'token_example' # str |  (optional)
type = 'type_example' # str | Event type. (optional)

try: 
    api_response = api_instance.weave_events_list(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, command_id=command_id, device_id=device_id, end_time_ms=end_time_ms, hl=hl, max_results=max_results, start_index=start_index, start_time_ms=start_time_ms, token=token, type=type)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling EventsApi->weave_events_list: %s\n" % e)
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
 **command_id** | [**list[str]**](str.md)| Affected command id. | [optional] 
 **device_id** | [**list[str]**](str.md)| Sending or affected device id. | [optional] 
 **end_time_ms** | **str**| End of time range in ms since epoch. | [optional] 
 **hl** | **str**| Specifies the language code that should be used for text values in the API response. | [optional] 
 **max_results** | **int**|  | [optional] 
 **start_index** | **int**|  | [optional] 
 **start_time_ms** | **str**| Start of time range in ms since epoch. | [optional] 
 **token** | **str**|  | [optional] 
 **type** | **str**| Event type. | [optional] 

### Return type

[**EventsListResponse**](EventsListResponse.md)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **weave_events_record_device_events**
> weave_events_record_device_events(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)



Enables or disables recording of a particular device's events based on a boolean parameter. Enabled by default.

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
api_instance = swagger_client.EventsApi()
alt = 'json' # str | Data format for the response. (optional) (default to json)
fields = 'fields_example' # str | Selector specifying which fields to include in a partial response. (optional)
key = 'key_example' # str | API key. Your API key identifies your project and provides you with API access, quota, and reports. Required unless you provide an OAuth 2.0 token. (optional)
oauth_token = 'oauth_token_example' # str | OAuth 2.0 token for the current user. (optional)
pretty_print = true # bool | Returns response with indentations and line breaks. (optional) (default to true)
quota_user = 'quota_user_example' # str | Available to use for quota purposes for server-side applications. Can be any arbitrary string assigned to a user, but should not exceed 40 characters. Overrides userIp if both are provided. (optional)
user_ip = 'user_ip_example' # str | IP address of the site where the request originates. Use this if you want to enforce per-user limits. (optional)
hl = 'hl_example' # str | Specifies the language code that should be used for text values in the API response. (optional)
body = swagger_client.EventsRecordDeviceEventsRequest() # EventsRecordDeviceEventsRequest |  (optional)

try: 
    api_instance.weave_events_record_device_events(alt=alt, fields=fields, key=key, oauth_token=oauth_token, pretty_print=pretty_print, quota_user=quota_user, user_ip=user_ip, hl=hl, body=body)
except ApiException as e:
    print("Exception when calling EventsApi->weave_events_record_device_events: %s\n" % e)
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
 **body** | [**EventsRecordDeviceEventsRequest**](EventsRecordDeviceEventsRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[apiKey](../README.md#apiKey)

### HTTP request headers

 - **Content-Type**: application/json, application/protobuf, application/xml
 - **Accept**: application/json, application/protobuf, application/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

