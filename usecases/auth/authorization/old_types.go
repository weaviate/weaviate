//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authorization

import (
	"errors"
)

type DomainsAndVerbs struct {
	Domains []Domain
	Verbs   []string
}

// TODO add translation layer between weaviate and Casbin permissions
var BuiltInRoles = map[string]DomainsAndVerbs{
	"viewer": {Domains: Domains, Verbs: readOnlyVerbs()},
	"editor": {Domains: Domains, Verbs: editorVerbs()},
	"admin":  {Domains: Domains, Verbs: adminVerbs()},
}

func readOnlyVerbs() []string {
	return []string{READ}
}

func editorVerbs() []string {
	return []string{CRU}
}

func adminVerbs() []string {
	return []string{CRUD}
}

type Action interface {
	Verbs() []string
}

func Verbs[T Action](a T) []string {
	return a.Verbs()
}

// ActionsByDomain
type (
	Read   string
	Write  string
	Update string
	Delete string
	All    string
)

func (r Read) Verbs() []string {
	return []string{READ}
}

func (r Write) Verbs() []string {
	return []string{CREATE}
}

func (r Update) Verbs() []string {
	return []string{UPDATE}
}

func (r Delete) Verbs() []string {
	return []string{DELETE}
}

func (r All) Verbs() []string {
	return []string{CRUD}
}

const (
	ManageRoles   All  = "manage_roles"
	ReadRoles     Read = "read_roles"
	ManageCluster All  = "manage_cluster"

	CreateCollections Write  = "create_collections"
	ReadCollections   Read   = "read_collections"
	UpdateCollections Update = "update_collections"
	DeleteCollections Delete = "delete_collections"

	CreateTenants Write  = "create_tenants"
	ReadTenants   Read   = "read_tenants"
	UpdateTenants Update = "update_tenants"
	DeleteTenants Delete = "delete_tenants"

	// not in first version
	CreateObjectsC Write  = "create_objects_collection"
	ReadObjectsC   Read   = "read_objects_collection"
	UpdateObjectsC Update = "update_objects_collection"
	DeleteObjectsC Delete = "delete_objects_collection"

	CreateObjectT  Write  = "create_objects_tenant"
	ReadObjectsT   Read   = "read_objects_tenant"
	UpdateObjectsT Update = "update_objects_tenant"
	DeleteObjectsT Delete = "delete_objects_tenant"
)

func AllActionsForDomain(domain Domain) ([]Action, []string) {
	var actions []Action
	var names []string
	for name, action := range ActionsByDomain[domain] {
		actions = append(actions, action)
		names = append(names, name)
	}
	return actions, names
}

type Domain string

const (
	RolesD             Domain = "roles"
	ClusterD           Domain = "cluster"
	CollectionD        Domain = "collections"
	TenantD            Domain = "tenants"
	ObjectsCollectionD Domain = "objects_collection"
	ObjectsTenantsD    Domain = "objects_tenant"
)

var Domains []Domain = []Domain{RolesD, ClusterD, CollectionD, TenantD, ObjectsCollectionD, ObjectsTenantsD}

func (d Domain) String() string {
	return string(d)
}

func ToDomain(s string) (Domain, error) {
	switch Domain(s) {
	case RolesD, ClusterD, CollectionD, TenantD, ObjectsCollectionD, ObjectsTenantsD:
		return Domain(s), nil
	}
	return "", errors.New("invalid status: " + s)
}

var (
	ActionsByDomain = map[Domain]map[string]Action{
		RolesD: {
			string(ManageRoles): ManageRoles,
			string(ReadRoles):   ReadRoles,
		},
		ClusterD: {
			string(ManageCluster): ManageCluster,
		},
		CollectionD: {
			string(CreateCollections): CreateCollections,
			string(ReadCollections):   ReadCollections,
			string(UpdateCollections): UpdateCollections,
			string(DeleteCollections): DeleteCollections,
		},
		TenantD: {
			string(CreateTenants): CreateTenants,
			string(ReadTenants):   ReadTenants,
			string(UpdateTenants): UpdateTenants,
			string(DeleteTenants): DeleteTenants,
		},
		ObjectsCollectionD: {
			string(CreateObjectsC): CreateObjectsC,
			string(ReadObjectsC):   ReadObjectsC,
			string(UpdateObjectsC): UpdateObjectsC,
			string(DeleteObjectsC): DeleteObjectsC,
		},
		ObjectsTenantsD: {
			string(CreateObjectT):  CreateObjectT,
			string(ReadObjectsT):   ReadObjectsT,
			string(UpdateObjectsT): UpdateObjectsT,
			string(DeleteObjectsT): DeleteObjectsT,
		},
	}

	DomainByAction = map[string]Domain{
		string(ManageRoles):       RolesD,
		string(ReadRoles):         RolesD,
		string(ManageCluster):     ClusterD,
		string(CreateCollections): CollectionD,
		string(ReadCollections):   CollectionD,
		string(UpdateCollections): CollectionD,
		string(DeleteCollections): CollectionD,
		string(CreateTenants):     TenantD,
		string(ReadTenants):       TenantD,
		string(UpdateTenants):     TenantD,
		string(DeleteTenants):     TenantD,
		string(CreateObjectT):     ObjectsTenantsD,
		string(ReadObjectsT):      ObjectsTenantsD,
		string(UpdateObjectsT):    ObjectsTenantsD,
		string(DeleteObjectsT):    ObjectsTenantsD,
		string(CreateObjectsC):    ObjectsCollectionD,
		string(ReadObjectsC):      ObjectsCollectionD,
		string(UpdateObjectsC):    ObjectsCollectionD,
		string(DeleteObjectsC):    ObjectsCollectionD,
	}
)
