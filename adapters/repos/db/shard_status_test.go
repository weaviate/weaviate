//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

// TODO: Mock a fake store+buckets
//func TestShardsStatus_Get_Update(t *testing.T) {
//	s := &Shard{
//		name:       "testshard",
//		statusLock: &sync.Mutex{},
//		status:     storagestate.StatusReady,
//	}
//
//	t.Run("get status", func(t *testing.T) {
//		status := s.getStatus()
//		require.Equal(t, storagestate.StatusReady, status)
//	})
//
//	t.Run("update status success", func(t *testing.T) {
//		status := []string{
//			"ReadOnly",
//			"readonly",
//			"READy",
//			"ready",
//			storagestate.StatusReady.String(),
//			storagestate.StatusReadOnly.String(),
//		}
//
//		for _, st := range status {
//			err := s.updateStatus(st)
//			require.Nil(t, err)
//
//			status := s.getStatus()
//			require.Equal(t, strings.ToUpper(st), status)
//		}
//	})
//
//	t.Run("update status failure", func(t *testing.T) {
//		err := s.updateStatus("invalid_status")
//		require.EqualError(t, err, "'invalid_status' is not a valid shard status")
//	})
//}
