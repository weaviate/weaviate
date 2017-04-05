# Event

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**command_patch** | [**EventCommandPatch**](EventCommandPatch.md) |  | [optional] 
**connection_status** | **str** | New device connection state (if connectivity change event). | [optional] 
**device_id** | **str** | The device that was affected by this event. | [optional] 
**id** | **str** | ID of the event. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#event\&quot;. | [optional] [default to 'weave#event']
**state_patch** | [**JsonObject**](JsonObject.md) |  | [optional] 
**time_ms** | **str** | Time the event was generated in milliseconds since epoch UTC. | [optional] 
**type** | **str** | Type of the event. | [optional] 
**user_email** | **str** | User that caused the event (if applicable). | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


