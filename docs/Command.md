# Command

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**blob_parameters** | [**JsonObject**](JsonObject.md) |  | [optional] 
**blob_results** | [**JsonObject**](JsonObject.md) |  | [optional] 
**component** | **str** | Component name paths separated by &#39;/&#39;. | [optional] 
**creation_time_ms** | **str** | Timestamp since epoch of a creation of a command. | [optional] 
**creator_email** | **str** | User that created the command (not applicable if the user is deleted). | [optional] 
**device_id** | **str** | Device ID that this command belongs to. | [optional] 
**error** | [**CommandError**](CommandError.md) |  | [optional] 
**expiration_time_ms** | **str** | Timestamp since epoch of command expiration. | [optional] 
**expiration_timeout_ms** | **str** | Expiration timeout for the command since its creation, 10 seconds min, 30 days max. | [optional] 
**id** | **str** | Unique command ID. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#command\&quot;. | [optional] [default to 'weave#command']
**last_update_time_ms** | **str** | Timestamp since epoch of last update made to the command. | [optional] 
**name** | **str** | Full command name, including trait. | [optional] 
**parameters** | [**JsonObject**](JsonObject.md) |  | [optional] 
**progress** | [**JsonObject**](JsonObject.md) |  | [optional] 
**results** | [**JsonObject**](JsonObject.md) |  | [optional] 
**state** | **str** | Current command state. | [optional] 
**user_action** | **str** | Pending command state that is not acknowledged by the device yet. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


