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

package cyclemanager

import (
	"errors"
	"fmt"
)

var ErrorCallbackNotFound = errors.New("callback not found")
var (
	formatActivateCallback   = "activating callback '%s' of '%s' failed: %w"
	formatDeactivateCallback = "deactivating callback '%s' of '%s' failed: %w"
	formatUnregisterCallback = "unregistering callback '%s' of '%s' failed: %w"
)

func errorActivateCallback(callbackCustomId, callbacksCustomId string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(formatActivateCallback, callbackCustomId, callbacksCustomId, err)
}

func errorDeactivateCallback(callbackCustomId, callbacksCustomId string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(formatDeactivateCallback, callbackCustomId, callbacksCustomId, err)
}

func errorUnregisterCallback(callbackCustomId, callbacksCustomId string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(formatUnregisterCallback, callbackCustomId, callbacksCustomId, err)
}
