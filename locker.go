package main

import "sync"

var (
	locker      map[string]*sync.Mutex
	lockerMutex sync.Mutex
)

func getLock(key string) *sync.Mutex {
	lockerMutex.Lock()
	defer lockerMutex.Unlock()
	if locker == nil {
		locker = map[string]*sync.Mutex{}
	}
	if _, found := locker[key]; !found {
		locker[key] = &sync.Mutex{}
	}
	return locker[key]
}

func lock(key string) {
	getLock(key).Lock()
}

func unlock(key string) {
	getLock(key).Unlock()
}
