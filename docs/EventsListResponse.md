# EventsListResponse

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**events** | [**list[Event]**](Event.md) | The actual list of events in reverse chronological order. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#eventsListResponse\&quot;. | [optional] [default to 'weave#eventsListResponse']
**next_page_token** | **str** | Token for the next page of events. | [optional] 
**total_results** | **int** | The total number of events for the query. The number of items in a response may be smaller due to paging. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


