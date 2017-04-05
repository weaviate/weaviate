# AclEntry

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cloud_access_revoked** | **bool** | Indicates whether the AclEntry has been revoked from the cloud and the user has no cloud access, but they still might have local auth tokens that are valid and can access the device and execute commands locally. See localAccessInfo for local auth details. | [optional] 
**creator_email** | **str** | User who created this entry. At the moment it is populated only when pending &#x3D;&#x3D; true. | [optional] 
**delegator** | **str** | User on behalf of whom the access is granted to the application. | [optional] 
**id** | **str** | Unique ACL entry ID. | [optional] 
**key** | **str** | Public access key value. Set only when scopeType is PUBLIC. | [optional] 
**kind** | **str** | Identifies what kind of resource this is. Value: the fixed string \&quot;weave#aclEntry\&quot;. | [optional] [default to 'weave#aclEntry']
**local_access_info** | [**LocalAccessInfo**](LocalAccessInfo.md) |  | [optional] 
**pending** | **bool** | Whether this ACL entry is pending for user reply to accept/reject it. | [optional] 
**privileges** | **list[str]** | Set of access privileges granted for this scope.  Valid values are:   - \&quot;modifyAcl\&quot;  - \&quot;viewAllEvents\&quot; | [optional] 
**revocation_time_ms** | **str** | Time in milliseconds since Unix Epoch indicating when the AclEntry was revoked. | [optional] 
**role** | **str** | Access role granted to this scope. | [optional] 
**scope_id** | **str** | Email address if scope type is user or group, domain name if scope type is a domain. | [optional] 
**scope_membership** | **str** | Type of membership the user has in the scope. | [optional] 
**scope_name** | **str** | Displayable scope name. | [optional] 
**scope_photo_url** | **str** | URL of this scope displayable photo. | [optional] 
**scope_type** | **str** | Type of the access scope. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


