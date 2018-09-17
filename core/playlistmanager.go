package core

import (
	"fmt"
	"sync"

	"github.com/ericxtang/m3u8"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/go-livepeer/net"
	ffmpeg "github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/stream"
)

const LIVE_LIST_LENGTH uint = 10

// XXX what if list will be longer?
const FULL_LIST_LENGTH uint = 131072

/*
	PlaylistManager manages playlists and data for one video stream, backed by one object storage.
*/
type PlaylistManager interface {
	ManifestID() ManifestID
	// Implicitly creates master and media playlists
	InsertHLSSegment(streamID StreamID, seg *stream.HLSSegment) (*net.TypedURI, error)
	// InsertLink link to segment that already in the storage
	// (Transcoder wrote segment in B's owned storage and returned link, B adds this link to video source)
	// returns true if it was acutally inserted
	InsertLink(turi *net.TypedURI, duration float64) (bool, error)

	GetHLSMasterPlaylist() *m3u8.MasterPlaylist

	GetHLSMediaPlaylist(streamID StreamID) *m3u8.MediaPlaylist

	GetSession() drivers.OSSession
	// Ends object storage's session
	EndSession()
}

type BasicPlaylistManager struct {
	storageSession drivers.OSSession
	manifestID     ManifestID
	// Live playlist used for broadcasting
	masterLivePList *m3u8.MasterPlaylist
	// Full playlist is stored into OS for later reference
	masterFullPList *m3u8.MasterPlaylist
	mediaLiveLists  map[StreamID]*m3u8.MediaPlaylist
	mediaFullLists  map[StreamID]*m3u8.MediaPlaylist
	mapSync         *sync.Mutex
}

// NewBasicPlaylistManager create new BasicPlaylistManager struct
func NewBasicPlaylistManager(manifestID ManifestID,
	storageSession drivers.OSSession) *BasicPlaylistManager {

	bplm := &BasicPlaylistManager{
		storageSession:  storageSession,
		manifestID:      manifestID,
		masterLivePList: m3u8.NewMasterPlaylist(),
		masterFullPList: m3u8.NewMasterPlaylist(),
		mediaLiveLists:  make(map[StreamID]*m3u8.MediaPlaylist),
		mediaFullLists:  make(map[StreamID]*m3u8.MediaPlaylist),
		mapSync:         &sync.Mutex{},
	}
	return bplm
}

func (mgr *BasicPlaylistManager) ManifestID() ManifestID {
	return mgr.manifestID
}

func (mgr *BasicPlaylistManager) GetSession() drivers.OSSession {
	return mgr.storageSession
}

// InsertHLSSegment ...
func (mgr *BasicPlaylistManager) InsertHLSSegment(streamID StreamID,
	seg *stream.HLSSegment) (*net.TypedURI, error) {

	if err := mgr.isRightStream(streamID); err != nil {
		return nil, err
	}
	sid := string(streamID)
	fmpl := mgr.getFullPL(streamID)
	if fmpl == nil {
		panic("no full media pl")
	}
	if mgr.hasSegment(fmpl, seg.SeqNo) {
		return nil, fmt.Errorf("Already saved")
	}
	turi, _, err := mgr.storageSession.SaveData(sid, seg.Name, seg.Data)
	if err != nil {
		glog.Error("Error saving segment", err)
	} else {
		turi.SeqNo = seg.SeqNo
		mgr.insertLink(turi, seg.Duration)
	}
	return turi, err
}

func (mgr *BasicPlaylistManager) isRightStream(streamID StreamID) error {
	mid, err := streamID.ManifestIDFromStreamID()
	if err != nil {
		return err
	}
	if mid != mgr.manifestID {
		return fmt.Errorf("Wrong sream id (%s), should be %s", streamID, mid)
	}
	return nil
}

func (mgr *BasicPlaylistManager) hasSegment(mpl *m3u8.MediaPlaylist, seqNum uint64) bool {
	for _, seg := range mpl.Segments {
		if seg != nil && seg.SeqId == seqNum {
			return true
		}
	}
	return false
}

func (mgr *BasicPlaylistManager) getLivePL(streamID StreamID) *m3u8.MediaPlaylist {
	mgr.mapSync.Lock()
	defer mgr.mapSync.Unlock()
	mpl := mgr.mediaLiveLists[streamID]
	var err error
	if mpl == nil {
		mpl, err = m3u8.NewMediaPlaylist(LIVE_LIST_LENGTH, LIVE_LIST_LENGTH)
		if err != nil {
			glog.Error(err)
		} else {
			mgr.mediaLiveLists[streamID] = mpl
			sid := string(streamID)
			vParams := ffmpeg.VideoProfileToVariantParams(ffmpeg.VideoProfileLookup[streamID.GetRendition()])
			url, err := mgr.savePlayList(mpl, streamID, sid+".m3u8")
			if err == nil {
				mgr.masterLivePList.Append(url, mpl, vParams)
				mid, _ := streamID.ManifestIDFromStreamID()
				_, _, err := mgr.storageSession.SaveData(sid, string(mid)+".m3u8",
					mgr.masterLivePList.Encode().Bytes())
				if err != nil {
					glog.Warningf("Error saving master playlist for %s stream %v", streamID, err)
				}
			}
		}
	}
	return mpl
}

func (mgr *BasicPlaylistManager) getFullPL(streamID StreamID) *m3u8.MediaPlaylist {
	glog.Infof("Getting full pl for %s", streamID)
	mgr.mapSync.Lock()
	defer mgr.mapSync.Unlock()
	mpl := mgr.mediaFullLists[streamID]
	var err error
	if mpl == nil {
		mpl, err = m3u8.NewMediaPlaylist(FULL_LIST_LENGTH, FULL_LIST_LENGTH)
		if err != nil {
			glog.Error(err)
		} else {
			mgr.mediaFullLists[streamID] = mpl
			sid := string(streamID)
			vParams := ffmpeg.VideoProfileToVariantParams(ffmpeg.VideoProfileLookup[streamID.GetRendition()])
			url, err := mgr.savePlayList(mpl, streamID, sid+"_full.m3u8")
			if err == nil {
				mgr.masterFullPList.Append(url, mpl, vParams)
				mid, _ := streamID.ManifestIDFromStreamID()
				_, _, err := mgr.storageSession.SaveData(sid, string(mid)+"_full.m3u8",
					mgr.masterFullPList.Encode().Bytes())
				if err != nil {
					glog.Warningf("Error saving full master playlist for %s stream %v", streamID, err)
				}
			}
		}
	}
	return mpl
}

func (mgr *BasicPlaylistManager) savePlayList(mpl *m3u8.MediaPlaylist, streamID StreamID,
	name string) (string, error) {

	_, url, err := mgr.storageSession.SaveData(string(streamID), name,
		mpl.Encode().Bytes())
	if err != nil {
		glog.Warningf("Error saving media playlist for %s stream %v", streamID, err)
	}
	return url, err
}

// InsertLink ...
func (mgr *BasicPlaylistManager) InsertLink(turi *net.TypedURI, duration float64) (bool, error) {
	if !mgr.storageSession.IsOwnStorage(turi) {
		return false, nil
	}
	return mgr.insertLink(turi, duration)
}

func (mgr *BasicPlaylistManager) insertLink(turi *net.TypedURI, duration float64) (bool, error) {
	tsid := StreamID(turi.StreamID)
	if err := mgr.isRightStream(tsid); err != nil {
		return false, err
	}
	mpl := mgr.getLivePL(tsid)
	mgr.addToMediaPlaylist(turi, duration, mpl, true)
	_, err := mgr.savePlayList(mpl, tsid, turi.StreamID+".m3u8")
	if err != nil {
		glog.Warningf("Error saving media playlist for %s stream %v", turi.StreamID, err)
	}
	mpl = mgr.getFullPL(tsid)
	mgr.addToMediaPlaylist(turi, duration, mpl, false)
	_, err = mgr.savePlayList(mpl, tsid, turi.StreamID+"_full.m3u8")
	if err != nil {
		glog.Warningf("Error saving full media playlist for %s stream %v", turi.StreamID, err)
	}
	return true, err
}

func (mgr *BasicPlaylistManager) mediaSegmentFromTuri(turi *net.TypedURI, duration float64) *m3u8.MediaSegment {
	mseg := new(m3u8.MediaSegment)
	mseg.URI = turi.UriInManifest
	mseg.SeqId = turi.SeqNo
	mseg.Duration = duration
	mseg.Title = turi.Title
	return mseg
}

func (mgr *BasicPlaylistManager) addToMediaPlaylist(turi *net.TypedURI, duration float64,
	mpl *m3u8.MediaPlaylist, live bool) error {

	mseg := mgr.mediaSegmentFromTuri(turi, duration)
	if live && mpl.Count() >= mpl.WinSize() {
		mpl.Remove()
	}
	if mpl.Count() == 0 {
		mpl.SeqNo = mseg.SeqId
	}
	return mpl.AppendSegment(mseg)
}

func (mgr *BasicPlaylistManager) makeMediaSegment(seg *stream.HLSSegment, url string) *m3u8.MediaSegment {
	mseg := new(m3u8.MediaSegment)
	mseg.URI = url
	mseg.Duration = seg.Duration
	mseg.Title = seg.Name
	mseg.SeqId = seg.SeqNo
	return mseg
}

// GetHLSMasterPlaylist ..
func (mgr *BasicPlaylistManager) GetHLSMasterPlaylist() *m3u8.MasterPlaylist {
	return mgr.masterLivePList
}

// GetHLSMediaPlaylist ...
func (mgr *BasicPlaylistManager) GetHLSMediaPlaylist(streamID StreamID) *m3u8.MediaPlaylist {
	return mgr.mediaLiveLists[streamID]
}

// EndSession ends object storage's session
func (mgr *BasicPlaylistManager) EndSession() {
	mgr.storageSession.EndSession()
}
