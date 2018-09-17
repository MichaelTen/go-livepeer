package drivers

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/net"
)

// LocalSaveMode if true local storage will be saving all the segments into special directory
var LocalSaveMode bool

// DataDir base dir to save data to
var DataDir string

var dataCacheLen = 12

type localOS struct {
	baseURL      string
	sessions     map[string]*localOSSession
	sessionsLock sync.RWMutex
}

type localOSSession struct {
	storage    *localOS
	jobID      int64
	nonce      uint64
	manifestID string
	baseURL    string
	dCache     map[string]*dataCache
	dLock      sync.RWMutex
	save       bool
	saveDir    string
}

func NewLocalDriver(baseURL string) *localOS {
	if LocalStorage != nil {
		panic("Should only be one instance of LocalStorage driver")
	}
	od := &localOS{
		baseURL:      baseURL,
		sessions:     make(map[string]*localOSSession),
		sessionsLock: sync.RWMutex{},
	}
	LocalStorage = od
	return od
}

// Clears memory cache from data belonging to manifest
func (ostore *localOS) EvictManifestData(mid string) {
	ostore.sessionsLock.Lock()
	delete(ostore.sessions, mid)
	ostore.sessionsLock.Unlock()
}

func (ostore *localOS) GetData(mid, name string) []byte {
	ostore.sessionsLock.RLock()
	session := ostore.sessions[mid]
	ostore.sessionsLock.RUnlock()
	if session != nil {
		return session.GetData(name)
	}
	return nil
}

func (ostore *localOS) IsExternal() bool {
	return false
}

func (ostore *localOS) StartSession(jobID int64, manifestID string, nonce uint64) OSSession {
	ostore.sessionsLock.Lock()
	defer ostore.sessionsLock.Unlock()
	if es, ok := ostore.sessions[manifestID]; ok {
		return es
	}
	s := localOSSession{
		storage:    ostore,
		jobID:      jobID,
		nonce:      nonce,
		manifestID: manifestID,
		baseURL:    ostore.baseURL,
		dCache:     make(map[string]*dataCache),
		dLock:      sync.RWMutex{},
		save:       LocalSaveMode,
		saveDir:    filepath.Join(DataDir, "saved", fmt.Sprintf("%05d_%s_%d", jobID, manifestID, nonce)),
	}
	ostore.sessions[manifestID] = &s

	if s.save {
		if _, err := os.Stat(s.saveDir); os.IsNotExist(err) {
			glog.Infof("Creating data dir: %v", s.saveDir)
			if err = os.MkdirAll(s.saveDir, 0755); err != nil {
				glog.Errorf("Error creating datadir: %v", err)
			}
		}
	}

	return &s
}

func (ostore *localOS) removeSession(session *localOSSession) {
	ostore.sessionsLock.Lock()
	delete(ostore.sessions, session.manifestID)
	ostore.sessionsLock.Unlock()
}

func (session *localOSSession) EndSession() {
	session.storage.removeSession(session)
}

func (session *localOSSession) IsOwnStorage(turi *net.TypedURI) bool {
	if turi.Storage != "local" {
		return false
	}
	if session.baseURL != "" && strings.Index(turi.Uri, session.baseURL) == 0 {
		return true
	}
	return false
}

func (session *localOSSession) GetInfo() *net.OSInfo {
	info := net.OSInfo{}
	return &info
}

func (session *localOSSession) GetData(name string) []byte {
	streamID := strings.Split(name, "_")[0]
	session.dLock.RLock()
	cache := session.dCache[streamID]
	defer session.dLock.RUnlock()
	if cache != nil {
		return cache.GetData(name)
	}
	return nil
}

func (session *localOSSession) getCacheForStream(streamID string) *dataCache {
	session.dLock.RLock()
	sc, ok := session.dCache[streamID]
	session.dLock.RUnlock()
	if !ok {
		sc = newDataCache(dataCacheLen)
		session.dLock.Lock()
		session.dCache[streamID] = sc
		session.dLock.Unlock()
	}
	return sc
}

func (session *localOSSession) getAbsoluteURL(name string) string {
	if session.baseURL != "" {
		return session.baseURL + "/stream/" + name
	}
	return name
}

func (session *localOSSession) SaveData(streamID, name string, data []byte) (*net.TypedURI, string, error) {
	dc := session.getCacheForStream(streamID)
	dc.Insert(name, data)
	abs := session.getAbsoluteURL(name)
	turl := &net.TypedURI{
		Storage:       "local",
		StreamID:      streamID,
		Uri:           abs,
		UriInManifest: name,
		Title:         name,
	}
	if session.save {
		fName := filepath.Join(session.saveDir, name)
		err := ioutil.WriteFile(fName, data, 0644)
		if err != nil {
			glog.Error(err)
		}
	}
	return turl, abs, nil
}

type dataCache struct {
	cacheLen int
	cache    []*dataCacheItem
}

type dataCacheItem struct {
	name string
	data []byte
}

func newDataCache(len int) *dataCache {
	return &dataCache{cacheLen: len, cache: make([]*dataCacheItem, 0)}
}

func (dc *dataCache) Insert(name string, data []byte) {
	// replace existing item
	for _, item := range dc.cache {
		if item.name == name {
			item.data = data
			return
		}
	}
	if len(dc.cache) >= dc.cacheLen {
		dc.cache = dc.cache[1:]
	}
	item := &dataCacheItem{name: name, data: data}
	dc.cache = append(dc.cache, item)
}

func (dc *dataCache) GetData(name string) []byte {
	for _, s := range dc.cache {
		if s.name == name {
			return s.data
		}
	}
	return nil
}
