//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package locks

import (
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	recipe "github.com/coreos/etcd/contrib/recipes"
	"github.com/sirupsen/logrus"
)

// EtcdLock is a distbributed lock based on etcd implementing
// locks.ConnectorSchemaLock
type EtcdLock struct {
	session *concurrency.Session
	logger  logrus.FieldLogger
	key     string
}

// NewEtcdLock for distributed locking of Connector and Schema
func NewEtcdLock(client *clientv3.Client, key string, logger logrus.FieldLogger) (*EtcdLock, error) {
	session, err := concurrency.NewSession(client)
	if err != nil {
		return nil, fmt.Errorf("could not create etcd session: %v", err)
	}

	el := &EtcdLock{
		session: session,
		key:     key,
		logger:  logger,
	}

	// Note that we are not creating an actual lock yet, but only a seesion. We
	// are only creating the actual RWMutex once the user calls one of the Lock()
	// or Unlock() methods.  Because of the distributed nature of the locks
	// (currently being managed with etcd) the local types should not span the
	// entire session. Each RWMutex holds the state of the key it uses for
	// locking. This means exactly this type has to be reused for unlocking, but
	// not used for anything else
	//
	// For example, the following is fine:
	//	m1 := recipe.NewRWMutex(/*...*/)
	//	m2 := recipe.NewRWMutex(/*...*/)
	//	m1.Lock()
	//	m1.Unlock()
	//	// now discard m1!
	//	m2.RLock()
	//	m2.RUnlock()
	//	// now discard m2!
	//
	// The above example might not be immediately intuitive, because when using
	// local mutexes it is extremly important to pass them by reference,
	// otherwise no syncing is possible. However on this remote lock it's exactly
	// the opposite.
	//
	// This code however would lead to problems:
	//	m := recipe.NewRWMutex(/*...*/)
	//
	//	go func() {
	// 		m.Lock()
	// 		// do something
	// 		m.Unlock()
	// 	}()
	//
	// 	go func() {
	// 		m.RLock()
	// 		// do something
	// 		m.RUnlock()
	// 	}()
	//
	// The above code segment would not work, because the first goroutine when
	// locking would set the internal m.myKey to whatever key and lease it got.
	// It is important that it keeps this state because it needs it to unlock the
	// same mutex, when doing m.Unlock(). However, the second goroutine would try
	// to do the same, it would set myKey to its own key+Lease on m.RLock(). This
	// means whichever goroutine of the two last called either Lock() or Unlock()
	// will have overwritten the internal key. Thus the other routine can never
	// be unlocked again, because on Unlock() or RUnlock() it would now try to
	// unlock with the other key.
	//
	// To fix the issue in Example 2, there should be no such thing as a "global"
	// Mutex, instead there should either be one per gouroutine or even better
	// yet, the mutexes should be created within the goroutines themselves.

	return el, nil
}

// LockConnector permits you to read and write class intances, but not make
// changes to the schema
func (l *EtcdLock) LockConnector() (func() error, error) {
	// Note that we are creating a new lock every time that LockConnector() is
	// called. This lock has a short lifespan. It's only other usage, other than
	// calling RLock() in the next line is to pass it to the user, so that they
	// can call RUnlock() exactly once on it. This might seem counter-intuitive,
	// as with local locks we need to make sure there is only one global lock.
	// Please see NewEtcdLock() for extensive documentation on why that would be
	// problematic in the case of the distributed lock.
	lock := recipe.NewRWMutex(l.session, l.key)
	if err := lock.RLock(); err != nil {
		return nil, fmt.Errorf("could not get connector lock: %s", err)
	}

	return func() error {
		if err := lock.RUnlock(); err != nil {
			l.logger.WithField("action", "etcd_lock_unlock_connector").
				WithField("event", "unlock_failed").
				WithError(err).
				Error("unlocking the connector lock failed. Warning: " +
					"This can lead to deadlock situations and will most likely have to be cleaned up manually")

			return err
		}

		return nil
	}, nil
}

// LockSchema permits you both read and write class instances, as well as
// modifying the schema. Regular queries that need only a connector lock will
// wait while the schmea lock is held
func (l *EtcdLock) LockSchema() (func() error, error) {
	// Note that we are creating a new lock every time that LockSchema() is
	// called. This lock has a short lifespan. It's only other usage, other than
	// calling Lock() in the next line is to pass it to the user, so that they
	// can call Unlock() exactly once on it. This might seem counter-intuitive,
	// as with local locks we need to make sure there is only one global lock.
	// Please see NewEtcdLock() for extensive documentation on why that would be
	// problematic in the case of the distributed lock.
	lock := recipe.NewRWMutex(l.session, l.key)
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("could not get schema lock: %s", err)
	}

	return func() error {
		if err := lock.Unlock(); err != nil {
			l.logger.WithField("action", "etcd_lock_unlock_schema").
				WithField("event", "unlock_failed").
				WithError(err).
				Error("unlocking the schema lock failed. Warning: " +
					"This can lead to deadlock situations and will most likely have to be cleaned up manually")

			return err
		}

		return nil
	}, nil
}
