//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package apikey

import (
	"context"
	"time"
)

type RemoteApiKey struct {
	apikey *DBUser
}

func NewRemoteApiKey(apikey *ApiKey) *RemoteApiKey {
	return &RemoteApiKey{apikey: apikey.Dynamic}
}

func (r *RemoteApiKey) GetUserStatus(ctx context.Context, users UserStatusRequest) (*UserStatusResponse, error) {
	r.apikey.UpdateLastUsedTimestamp(users.Users)

	if !users.ReturnStatus {
		return nil, nil
	}

	userIds := make([]string, 0, len(users.Users))
	for userId := range users.Users {
		userIds = append(userIds, userId)
	}
	userReturns, err := r.apikey.GetUsers(userIds...)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]time.Time, len(userReturns))
	for _, userReturn := range userReturns {
		ret[userReturn.Id] = userReturn.LastUsedAt
	}
	return &UserStatusResponse{Users: ret}, nil
}

type UserStatusResponse struct {
	Users map[string]time.Time
}

type UserStatusRequest struct {
	Users        map[string]time.Time
	ReturnStatus bool
}
