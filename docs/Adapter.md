# Adapter

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**activate_url** | **str** | URL to adapter web flow to activate the adapter. Deprecated, use the activationUrl returned in the response of the Adapters.activate API. | [optional] 
**activated** | **bool** | Whether this adapter has been activated for the current user. | [optional] 
**deactivate_url** | **str** | URL to adapter web flow to disconnect the adapter. Deprecated, the adapter will be notified via pubsub. | [optional] 
**display_name** | **str** | Display name of the adapter. | [optional] 
**icon_url** | **str** | URL to an icon that represents the adapter. | [optional] 
**id** | **str** | ID of the adapter. | [optional] 
**manage_url** | **str** | URL to adapter web flow to connect new devices. Only used for adapters that cannot automatically detect new devices. This field is returned only if the user has already activated the adapter. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


