package freetds

import (
	"sync"
)

type ParamsCache struct {
	cache map[string][]*spParam
	sync.RWMutex
}

func (pc *ParamsCache) Get(spName string) ([]*spParam, bool) {
	pc.RLock()
	defer pc.RUnlock()
	params, found := pc.cache[spName]
	return params, found
}

func (pc *ParamsCache) Set(spName string, params []*spParam) {
	pc.Lock()
	pc.cache[spName] = params
	pc.Unlock()
}

func NewParamsCache() *ParamsCache {
	return &ParamsCache{
		cache: make(map[string][]*spParam),
	}
}
