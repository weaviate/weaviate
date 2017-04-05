# ModelManifest

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**allowed_child_model_manifest_ids** | **list[str]** | For gateways, a list of device ids that are allowed to connect to it. | [optional] 
**applications** | [**list[Application]**](Application.md) | List of applications recommended to use with a device model. | [optional] 
**confirmation_image_url** | **str** | URL of image showing a confirmation button. | [optional] 
**device_image_url** | **str** | URL of device image. | [optional] 
**device_kind** | **str** | Device kind, see \&quot;deviceKind\&quot; field of the Device resource. See list of device kinds values. | [optional] 
**id** | **str** | Unique model manifest ID. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#modelManifest\&quot;. | [optional] [default to 'weave#modelManifest']
**model_description** | **str** | User readable device model description. | [optional] 
**model_name** | **str** | User readable device model name. | [optional] 
**oem_name** | **str** | User readable name of device model manufacturer. | [optional] 
**support_page_url** | **str** | URL of device support page. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


