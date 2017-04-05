# DeviceChannel

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**connection_status_hint** | **str** | Connection status hint, set by parent device. | [optional] 
**gcm_registration_id** | **str** | GCM registration ID. Required if device supports GCM delivery channel. | [optional] 
**gcm_sender_id** | **str** | GCM sender ID. For Chrome apps must be the same as sender ID during registration, usually API project ID. | [optional] 
**parent_id** | **str** | Parent device ID (aggregator) if it exists. | [optional] 
**pubsub** | [**DeviceChannelPubsub**](DeviceChannelPubsub.md) |  | [optional] 
**supported_type** | **str** | Channel type supported by device. Allowed types are: \&quot;gcm\&quot;, \&quot;xmpp\&quot;, \&quot;pubsub\&quot;, and \&quot;parent\&quot;. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


