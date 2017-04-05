# Device

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**adapter_id** | **str** | ID of the adapter that created this device. | [optional] 
**cert_fingerprint** | **str** | Deprecated, do not use. The HTTPS certificate fingerprint used to secure communication with device.. | [optional] 
**channel** | [**DeviceChannel**](DeviceChannel.md) |  | [optional] 
**command_defs** | [**dict(str, PackageDef)**](PackageDef.md) | Deprecated, use \&quot;traits\&quot; instead. Description of commands supported by the device. This field is writable only by devices. | [optional] 
**components** | [**JsonObject**](JsonObject.md) |  | [optional] 
**connection_status** | **str** | Device connection status. | [optional] 
**creation_time_ms** | **str** | Timestamp of creation of this device in milliseconds since epoch UTC. | [optional] 
**description** | **str** | User readable description of this device. | [optional] 
**device_kind** | **str** | Device kind. Deprecated, provide \&quot;modelManifestId\&quot; instead. See list of device kinds values. | [optional] 
**device_local_id** | **str** | Deprecated, do not use. The ID of the device for use on the local network. | [optional] 
**id** | **str** | Unique device ID. | [optional] 
**invitations** | [**list[Invitation]**](Invitation.md) | List of pending invitations for the currently logged-in user. | [optional] 
**is_event_recording_disabled** | **bool** | Indicates whether event recording is enabled or disabled for this device. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#device\&quot;. | [optional] [default to 'weave#device']
**labels** | [**list[AssociatedLabel]**](AssociatedLabel.md) | Any labels attached to the device. Use the addLabel and removeLabel APIs to modify this list. | [optional] 
**last_seen_time_ms** | **str** | Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type. | [optional] 
**last_update_time_ms** | **str** | Timestamp of the last device update in milliseconds since epoch UTC. | [optional] 
**last_use_time_ms** | **str** | Timestamp of the last device usage in milliseconds since epoch UTC. | [optional] 
**location** | **str** | Deprecated, do not use. User readable location of the device (name of the room, office number, building/floor, etc). | [optional] 
**model_manifest** | [**DeviceModelManifest**](DeviceModelManifest.md) |  | [optional] 
**model_manifest_id** | **str** | Model manifest ID of this device. | [optional] 
**name** | **str** | Name of this device provided by the manufacturer. | [optional] 
**nicknames** | **list[str]** | Nicknames of the device. Use the addNickname and removeNickname APIs to modify this list. | [optional] 
**owner** | **str** | E-mail address of the device owner. | [optional] 
**personalized_info** | [**DevicePersonalizedInfo**](DevicePersonalizedInfo.md) |  | [optional] 
**place_id** | **str** | ID of the place that this device belongs to. | [optional] 
**places_hints** | [**PlacesHints**](PlacesHints.md) |  | [optional] 
**room** | [**Room**](Room.md) |  | [optional] 
**serial_number** | **str** | Serial number of a device provided by its manufacturer. | [optional] 
**state** | [**JsonObject**](JsonObject.md) |  | [optional] 
**state_defs** | [**dict(str, StateDef)**](StateDef.md) | Deprecated, do not use. Description of the device state. This field is writable only by devices. | [optional] 
**tags** | **list[str]** | Custom free-form manufacturer tags. | [optional] 
**traits** | [**JsonObject**](JsonObject.md) |  | [optional] 
**ui_device_kind** | **str** | Device kind from the model manifest used in UI applications. See list of device kinds values. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


