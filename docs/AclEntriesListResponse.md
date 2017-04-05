# AclEntriesListResponse

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**acl_entries** | [**list[AclEntry]**](AclEntry.md) | The actual list of ACL entries. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#aclEntriesListResponse\&quot;. | [optional] [default to 'weave#aclEntriesListResponse']
**next_page_token** | **str** | Token corresponding to the next page of ACL entries. | [optional] 
**total_results** | **int** | The total number of ACL entries for the query. The number of items in a response may be smaller due to paging. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


