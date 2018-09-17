package core

import (
	"github.com/ericxtang/m3u8"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/lpms/stream"
)

// CombinedPlaylistManager simultaneously saves videos segments into  any number of playlist managers.
// This allows for the video stream to be saved into different object storages.
type CombinedPlaylistManager struct {
	manifestID ManifestID
	playlists  []PlaylistManager
}

func NewCombinedPlaylistManager(manifestID ManifestID,
	storageSessions []drivers.OSSession) PlaylistManager {

	if len(storageSessions) == 1 {
		return NewBasicPlaylistManager(manifestID, storageSessions[0])
	}

	cpmgr := &CombinedPlaylistManager{
		manifestID: manifestID,
		playlists:  make([]PlaylistManager, 0, len(storageSessions)),
	}
	for _, sess := range storageSessions {
		cpmgr.playlists = append(cpmgr.playlists, NewBasicPlaylistManager(manifestID, sess))
	}
	return cpmgr
}

func (mgr *CombinedPlaylistManager) ManifestID() ManifestID {
	return mgr.manifestID
}

func (mgr *CombinedPlaylistManager) GetSession() drivers.OSSession {
	return mgr.playlists[0].GetSession()
}

func (mgr *CombinedPlaylistManager) InsertHLSSegment(streamID StreamID,
	seg *stream.HLSSegment) (*net.TypedURI, error) {

	turi, err := mgr.playlists[0].InsertHLSSegment(streamID, seg)
	for _, p := range mgr.playlists[1:] {
		go func() {
			p.InsertHLSSegment(streamID, seg)
		}()
	}
	return turi, err
}

func (mgr *CombinedPlaylistManager) InsertLink(turi *net.TypedURI, duration float64) (bool, error) {
	inserted := true
	var err error
	for _, p := range mgr.playlists {
		var ins bool
		ins, err = p.InsertLink(turi, duration)
		inserted = inserted && ins
	}
	return inserted, err
}

func (mgr *CombinedPlaylistManager) GetHLSMasterPlaylist() *m3u8.MasterPlaylist {
	return mgr.playlists[0].GetHLSMasterPlaylist()
}

func (mgr *CombinedPlaylistManager) GetHLSMediaPlaylist(streamID StreamID) *m3u8.MediaPlaylist {
	return mgr.playlists[0].GetHLSMediaPlaylist(streamID)
}

// Ends object storage's session
func (mgr *CombinedPlaylistManager) EndSession() {
	for _, p := range mgr.playlists {
		p.EndSession()
	}
}
