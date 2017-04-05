# AdaptersActivateResponse

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**activation_id** | **str** | An ID that represents the link between the adapter and the user. The activationUrl will give this ID to the adapter for use in the accept API. | [optional] 
**activation_url** | **str** | A URL to the adapter where the user should go to complete the activation. The URL contains the activationId and the user&#39;s email address. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#adaptersActivateResponse\&quot;. | [optional] [default to 'weave#adaptersActivateResponse']

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


