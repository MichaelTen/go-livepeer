/*
Package server is the place we integrate the Livepeer node with the LPMS media server.
*/
package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"

	"github.com/cenkalti/backoff"
	"github.com/ericxtang/m3u8"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	ethTypes "github.com/livepeer/go-livepeer/eth/types"
	lpmscore "github.com/livepeer/lpms/core"
	ffmpeg "github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/segmenter"
	"github.com/livepeer/lpms/stream"
	"github.com/livepeer/lpms/vidplayer"
)

var ErrAlreadyExists = errors.New("StreamAlreadyExists")
var ErrRTMPPublish = errors.New("ErrRTMPPublish")
var ErrBroadcast = errors.New("ErrBroadcast")
var ErrHLSPlay = errors.New("ErrHLSPlay")
var ErrRTMPPlay = errors.New("ErrRTMPPlay")
var ErrRoundInit = errors.New("ErrRoundInit")

const HLSWaitInterval = time.Second
const HLSBufferCap = uint(43200) //12 hrs assuming 1s segment
const HLSBufferWindow = uint(5)

const SegLen = 4 * time.Second
const HLSUnsubWorkerFreq = time.Second * 5
const BroadcastRetry = 15 * time.Second

var HLSWaitTime = time.Second * 45
var BroadcastPrice = big.NewInt(1)
var BroadcastJobVideoProfiles = []ffmpeg.VideoProfile{ffmpeg.P240p30fps4x3, ffmpeg.P360p30fps16x9}
var MinDepositSegmentCount = int64(75)     // 5 mins assuming 4s segments
var MinJobBlocksRemaining = big.NewInt(40) // 10 mins assuming 15s blocks
var LastHLSStreamID core.StreamID
var LastManifestID core.ManifestID

type LivepeerServer struct {
	RTMPSegmenter   lpmscore.RTMPSegmenter
	LPMS            *lpmscore.LPMS
	LivepeerNode    *core.LivepeerNode
	VideoNonce      map[string]uint64
	VideoNonceLock  *sync.Mutex
	HttpMux         *http.ServeMux
	CurrentPlaylist core.PlaylistManager

	ExposeCurrentManifest bool

	rtmpStreams                map[core.StreamID]stream.RTMPVideoStream
	broadcastRtmpToManifestMap map[string]string
}

func NewLivepeerServer(rtmpAddr string, httpAddr string, lpNode *core.LivepeerNode) *LivepeerServer {
	opts := lpmscore.LPMSOpts{
		RtmpAddr: rtmpAddr, RtmpDisabled: true,
		HttpAddr: httpAddr,
		WorkDir:  lpNode.WorkDir,
	}
	switch lpNode.NodeType {
	case core.Broadcaster:
		opts.RtmpDisabled = false
	case core.Transcoder:
		opts.HttpMux = http.NewServeMux()
	}
	server := lpmscore.New(&opts)
	return &LivepeerServer{RTMPSegmenter: server, LPMS: server, LivepeerNode: lpNode, VideoNonce: map[string]uint64{}, VideoNonceLock: &sync.Mutex{}, HttpMux: opts.HttpMux, rtmpStreams: make(map[core.StreamID]stream.RTMPVideoStream), broadcastRtmpToManifestMap: make(map[string]string)}
}

//StartServer starts the LPMS server
func (s *LivepeerServer) StartMediaServer(ctx context.Context, maxPricePerSegment *big.Int, transcodingOptions string) error {
	if s.LivepeerNode.Eth != nil {
		BroadcastPrice = maxPricePerSegment
		bProfiles := make([]ffmpeg.VideoProfile, 0)
		for _, opt := range strings.Split(transcodingOptions, ",") {
			p, ok := ffmpeg.VideoProfileLookup[strings.TrimSpace(opt)]
			if ok {
				bProfiles = append(bProfiles, p)
			}
		}
		BroadcastJobVideoProfiles = bProfiles
		glog.V(common.SHORT).Infof("Transcode Job Price: %v, Transcode Job Type: %v", BroadcastPrice, BroadcastJobVideoProfiles)
	}

	//LPMS handlers for handling RTMP video
	s.LPMS.HandleRTMPPublish(createRTMPStreamIDHandler(s), gotRTMPStreamHandler(s), endRTMPStreamHandler(s))
	s.LPMS.HandleRTMPPlay(getRTMPStreamHandler(s))

	//LPMS hanlder for handling HLS video play
	s.LPMS.HandleHLSPlay(getHLSMasterPlaylistHandler(s), getHLSMediaPlaylistHandler(s), getHLSSegmentHandler(s))

	//Start the LPMS server
	lpmsCtx, cancel := context.WithCancel(context.Background())
	ec := make(chan error, 1)
	go func() {
		if err := s.LPMS.Start(lpmsCtx); err != nil {
			// typically triggered if there's an error with broadcaster LPMS
			// transcoder LPMS should return without an error
			ec <- s.LPMS.Start(lpmsCtx)
		}
	}()

	select {
	case err := <-ec:
		glog.Infof("LPMS Server Error: %v.  Quitting...", err)
		cancel()
		return err
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
}

//RTMP Publish Handlers
func createRTMPStreamIDHandler(s *LivepeerServer) func(url *url.URL) (strmID string) {
	return func(url *url.URL) (strmID string) {
		id, err := core.MakeStreamID(core.RandomVideoID(), "RTMP")
		if err != nil {
			glog.Errorf("Error making stream ID")
			return ""
		}
		return id.String()
	}
}

func (s *LivepeerServer) startBroadcast(job *ethTypes.Job, manifest *m3u8.MasterPlaylist,
	nonce uint64, profileName string) (Broadcaster, error) {

	tca := job.TranscoderAddress
	serviceUri, err := s.LivepeerNode.Eth.GetServiceURI(tca)
	if err != nil || serviceUri == "" {
		glog.Errorf("Unable to retrieve the Service URI for %v: %v", tca.Hex(), err)
		if err == nil {
			err = fmt.Errorf("Empty Service URI")
		}
		return nil, err
	}
	mid, err := core.StreamID(job.StreamId).ManifestIDFromStreamID()
	if err != nil {
		return nil, err
	}
	rpcBcast := core.NewBroadcaster(s.LivepeerNode, job)
	jid := job.JobId.Int64()
	// XXX can we generalize this?
	if drivers.Storages[0].IsExternal() {
		iosSession := drivers.Storages[0].StartSession(jid, string(mid), nonce)
		rpcBcast.SetOutputOS(iosSession)
	}

	err = StartBroadcastClient(rpcBcast, serviceUri, nonce)
	if err != nil {
		glog.Error("Unable to start broadcast client for ", job.JobId)
		if monitor.Enabled {
			monitor.LogStartBroadcastClientFailed(nonce, serviceUri, tca.Hex(), job.JobId.Uint64(), err.Error())
		}
		return nil, err
	}
	tinfo := rpcBcast.GetTranscoderInfo()
	if tinfo.PreferredIOS != nil {
		transcodersIOS := drivers.NewDriver(tinfo.PreferredIOS.Storage, tinfo.PreferredIOS)
		transcodersIOSsession := transcodersIOS.StartSession(jid, string(mid), nonce)
		rpcBcast.SetInputOS(transcodersIOSsession)
	}
	return rpcBcast, nil
}

func gotRTMPStreamHandler(s *LivepeerServer) func(url *url.URL, rtmpStrm stream.RTMPVideoStream) (err error) {
	return func(url *url.URL, rtmpStrm stream.RTMPVideoStream) (err error) {
		// For now, only allow a single stream
		if len(s.rtmpStreams) > 0 {
			return ErrAlreadyExists
		}

		s.VideoNonceLock.Lock()
		nonce, ok := s.VideoNonce[rtmpStrm.GetStreamID()]
		if !ok {
			nonce = rand.Uint64()
			s.VideoNonce[rtmpStrm.GetStreamID()] = nonce
		} else {
			// We can only have one concurrent stream for now
			return ErrAlreadyExists
		}
		s.VideoNonceLock.Unlock()

		//We try to automatically determine the video profile from the RTMP stream.
		var vProfile ffmpeg.VideoProfile
		resolution := fmt.Sprintf("%vx%v", rtmpStrm.Width(), rtmpStrm.Height())
		for _, vp := range ffmpeg.VideoProfileLookup {
			if vp.Resolution == resolution {
				vProfile = vp
				break
			}
		}
		if vProfile.Name == "" {
			vProfile = ffmpeg.P720p30fps16x9
			glog.V(common.SHORT).Infof("Cannot automatically detect the video profile - setting it to %v", vProfile)
		}

		jobStreamId := ""
		startSeq := 0
		manifest := m3u8.NewMasterPlaylist()
		var jobId *big.Int
		var rpcBcast Broadcaster
		if s.LivepeerNode.Eth != nil {

			// First check if we already have a job that can be reused
			blknum, err := s.LivepeerNode.Eth.LatestBlockNum()
			if err != nil {
				glog.Error("Unable to fetch latest block number ", err)
				return err
			}

			until := big.NewInt(0).Add(blknum, MinJobBlocksRemaining)
			bcasts, err := s.LivepeerNode.Database.ActiveBroadcasts(until)
			if err != nil {
				glog.Error("Unable to find active broadcasts ", err)
			}

			getReusableBroadcast := func(b *common.DBJob) (*ethTypes.Job, bool) {
				job := common.DBJobToEthJob(b)

				// Transcoder must still be registered
				if t, err := s.LivepeerNode.Eth.GetTranscoder(b.Transcoder); err != nil || t.Status == "Not Registered" {
					return job, false
				}

				// Transcode options must be the same
				if bytes.Compare(common.ProfilesToTranscodeOpts(job.Profiles), common.ProfilesToTranscodeOpts(BroadcastJobVideoProfiles)) != 0 {
					return job, false
				}

				// Existing job with a max price per segment <= current desired price is acceptable
				return job, job.MaxPricePerSegment.Cmp(BroadcastPrice) <= 0
			}

			for _, b := range bcasts {
				// check if job has acceptable price, transcoding options and valid assigned transcoder
				if job, reusable := getReusableBroadcast(b); reusable {
					rpcBcast, err = s.startBroadcast(job, manifest, nonce, vProfile.Name)
					if err == nil {
						startSeq = int(b.Segments) + 1
						jobId = big.NewInt(b.ID)
						jobStreamId = job.StreamId
						if monitor.Enabled {
							monitor.LogJobReusedEvent(job, startSeq, nonce)
						}
						break
					}
				}
			}
		}

		if s.LivepeerNode.Eth != nil && jobId == nil {

			//Check if round is initialized
			initialized, err := s.LivepeerNode.Eth.CurrentRoundInitialized()
			if err != nil {
				glog.Errorf("Could not check whether round was initialized: %v", err)
				return err
			}
			if !initialized {
				glog.Infof("Round was uninitialized, can't create job. Please try again in a few blocks.")
				// todo send to metrics ?
				return ErrRoundInit
			}
			// Check deposit
			deposit, err := s.LivepeerNode.Eth.BroadcasterDeposit(s.LivepeerNode.Eth.Account().Address)
			if err != nil {
				glog.Errorf("Error getting deposit: %v", err)
				return ErrBroadcast
			}

			glog.V(common.SHORT).Infof("Current deposit is: %v", deposit)

			minDeposit := big.NewInt(0).Mul(BroadcastPrice, big.NewInt(MinDepositSegmentCount))
			if deposit.Cmp(minDeposit) < 0 {
				glog.Errorf("Low deposit (%v) - cannot start broadcast session.  Need at least %v", deposit, minDeposit)
				if monitor.Enabled {
					monitor.LogStreamCreateFailed(nonce, "LowDeposit")
				}
				return ErrBroadcast
			}
		}

		//Check if stream ID already exists
		if _, ok := s.rtmpStreams[core.StreamID(rtmpStrm.GetStreamID())]; ok {
			return ErrAlreadyExists
		}

		//Add stream to stream store
		s.rtmpStreams[core.StreamID(rtmpStrm.GetStreamID())] = rtmpStrm

		//Create a HLS StreamID
		//If streamID is passed in, use that one
		//Else if we are reusing an active broadcast, use the old streamID
		//Else generate a random ID
		hlsStrmID := core.StreamID(url.Query().Get("hlsStrmID"))
		if hlsStrmID == "" && jobId != nil && jobStreamId != "" {
			hlsStrmID = core.StreamID(jobStreamId)
		} else if hlsStrmID == "" {
			hlsStrmID, err = core.MakeStreamID(core.RandomVideoID(), vProfile.Name)
			if err != nil {
				glog.Errorf("Error making stream ID")
				return ErrRTMPPublish
			}
		}

		LastHLSStreamID = hlsStrmID

		streamStarted := false
		httpc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
		//Segment the stream, insert the segments into the broadcaster
		go func(rtmpStrm stream.RTMPVideoStream) {
			hlsStrm := stream.NewBasicHLSVideoStream(string(hlsStrmID), stream.DefaultHLSStreamWin)
			hlsStrm.SetSubscriber(func(seg *stream.HLSSegment, eof bool) {
				if eof {
					// XXX update HLS manifest
					return
				}

				if streamStarted == false {
					streamStarted = true
					if monitor.Enabled {
						monitor.LogStreamStartedEvent(nonce)
					}
				}
				if monitor.Enabled {
					monitor.LogSegmentEmerged(nonce, seg.SeqNo)
				}

				if jobId != nil {
					s.LivepeerNode.Database.SetSegmentCount(jobId, int64(seg.SeqNo))
				}
				cpl := s.CurrentPlaylist
				if cpl == nil {
					glog.Warning("Curren playlist not created")
					panic("no playlist")
				}

				stored, err := cpl.InsertHLSSegment(hlsStrmID, seg)
				if err != nil {
					glog.Error("Error saving segment:", err)
				}

				if rpcBcast != nil {
					go func() {
						// send segment to the transcoder
						glog.V(common.DEBUG).Infof("starting to submit segment %d with name %s", seg.SeqNo, seg.Name)
						// input storage that was negotiated with orchestrator (only in case it is not owned by B)
						ios := rpcBcast.GetInputOS()
						var turl *net.TypedURI
						var err error
						var osInfo *net.OSInfo
						if ios != nil {
							turl, _, err = ios.SaveData(string(hlsStrmID), seg.Name, seg.Data)
							if err != nil {
								glog.Error("Error saving segment to OS", err)
								if monitor.Enabled {
									monitor.LogSegmentUploadFailed(nonce, seg.SeqNo, err.Error())
								}
								return
							}
						} else if drivers.IsExternal(stored) {
							turl = stored
							cpl := s.CurrentPlaylist
							if cpl != nil {
								storageSession := cpl.GetSession()
								if storageSession.IsOwnStorage(turl) {
									// need to send credentials for our own storage
									osInfo = storageSession.GetInfo()
								}
							}
						}
						segmentInfo := &net.SegmentInfo{
							Turi: turl,
							SessionInfo: &net.SessionInfo{
								Nonce:  nonce,
								OsInfo: osInfo,
							},
						}
						res, err := SubmitSegment(rpcBcast, seg, segmentInfo)
						if err != nil {
							return
						}

						// download transcoded segments from the transcoder
						gotErr := false // only send one error msg per segment list
						errFunc := func(ev, url string, err error) {
							glog.Errorf("%v error with segment %v: %v (URL: %v)", ev, seg.SeqNo, err, url)
							if monitor.Enabled && !gotErr {
								monitor.LogSegmentTranscodeFailed(ev, nonce, seg.SeqNo, err)
								gotErr = true
							}
						}
						dlFunc := func(url string) {
							glog.V(common.DEBUG).Infof("starting legacy download %s", url)
							res, err := httpc.Get(url)
							if err != nil {
								errFunc("Retrieval", url, err)
								return
							}
							// TODO persist this to disk.
							// Should make videocache read from disk as well
							data, err := ioutil.ReadAll(res.Body)
							if err != nil {
								errFunc("Download", url, err)
								return
							}
							sid, err := parseStreamID(url)
							if err != nil {
								errFunc("StreamID", url, err)
								return
							}
							glog.V(common.DEBUG).Infof("legacy downloaded sid %s seg name %s", sid, parseSegName(url))
							newSeg := &stream.HLSSegment{SeqNo: seg.SeqNo, Name: parseSegName(url), Data: data, Duration: seg.Duration}
							cpl := s.CurrentPlaylist
							if cpl != nil {
								cpl.InsertHLSSegment(sid, newSeg)
							}
						}
						dlFromOSFunc := func(turi *net.TypedURI) {
							data, err := drivers.GetSegmentData(turi)
							if err != nil {
								errFunc("Download", turi.Uri, err)
								return
							}
							glog.V(common.DEBUG).Infof("segment downloaded from %s sid %s seg name %s",
								turi.Storage, turi.StreamID, parseSegNameSimple(turi.Uri))
							newSeg := &stream.HLSSegment{SeqNo: seg.SeqNo,
								Name: parseSegNameSimple(turi.Uri),
								Data: data, Duration: seg.Duration}
							cpl := s.CurrentPlaylist
							if cpl != nil {
								cpl.InsertHLSSegment(core.StreamID(turi.StreamID), newSeg)
							}
						}
						glog.V(common.DEBUG).Infof("Got result from transcode: %+v", res)
						for _, v := range res.Segments {
							if v.Turl == nil {
								// old orchestrator
								go dlFunc(v.Url)
							} else {
								cpl := s.CurrentPlaylist
								if cpl != nil {
									inserted, err := cpl.InsertLink(v.Turl, seg.Duration)
									if err != nil {
										glog.Error("Error inserting link", v.Turl, err)
									}
									if !inserted {
										go dlFromOSFunc(v.Turl)
									}
								}
							}
						}
					}()
				}
			})

			segOptions := segmenter.SegmenterOptions{
				StartSeq:  startSeq,
				SegLength: SegLen,
			}
			err := s.RTMPSegmenter.SegmentRTMPToHLS(context.Background(), rtmpStrm, hlsStrm, segOptions)
			if err != nil {
				// TODO update manifest

				// Stop the incoming RTMP connection.
				// TODO retry segmentation if err != SegmenterTimeout; may be recoverable
				rtmpStrm.Close()
			}

		}(rtmpStrm)

		//Create the manifest and broadcast it (so the video can be consumed by itself without transcoding)
		mid, err := core.MakeManifestID(hlsStrmID.GetVideoID())
		if err != nil {
			glog.Errorf("Error creating manifest id: %v", err)
			return ErrRTMPPublish
		}
		LastManifestID = mid

		var jid int64
		if jobId != nil {
			jid = jobId.Int64()
		}
		if s.CurrentPlaylist != nil {
			s.CurrentPlaylist.EndSession()
		}
		storageSessions := make([]drivers.OSSession, 0, len(drivers.Storages))
		for _, os := range drivers.Storages {
			storageSessions = append(storageSessions, os.StartSession(jid, string(mid), nonce))
		}
		s.CurrentPlaylist = core.NewCombinedPlaylistManager(mid, storageSessions)

		if monitor.Enabled {
			monitor.LogStreamCreatedEvent(hlsStrmID.String(), nonce)
		}

		glog.Infof("\n\nVideo Created With ManifestID: %v\n\n", mid)
		glog.V(common.SHORT).Infof("\n\nhlsStrmID: %v\n\n", hlsStrmID)

		s.broadcastRtmpToManifestMap[rtmpStrm.GetStreamID()] = string(mid)

		if jobId == nil && s.LivepeerNode.Eth != nil {
			//Create Transcode Job Onchain
			go func() {
				job, err := s.LivepeerNode.CreateTranscodeJob(hlsStrmID, BroadcastJobVideoProfiles, BroadcastPrice)
				if err != nil {
					return // XXX feed back error?
				}
				jobId = job.JobId
				if monitor.Enabled {
					monitor.LogJobCreatedEvent(job, nonce)
				}

				// Connect to the orchestrator. If it fails, retry for as long
				// as the RTMP stream is alive; maybe the orchestrator hasn't
				// received the block containing the job yet
				broadcastFunc := func() error {
					rpcBcast, err = s.startBroadcast(job, manifest, nonce, vProfile.Name)
					if err != nil {
						// Should be logged upstream
					}
					s.VideoNonceLock.Lock()
					_, active := s.VideoNonce[rtmpStrm.GetStreamID()]
					s.VideoNonceLock.Unlock()
					if active {
						return err
					}
					return nil // stop if inactive
				}
				expb := backoff.NewExponentialBackOff()
				expb.MaxInterval = BroadcastRetry
				expb.MaxElapsedTime = 0
				backoff.Retry(broadcastFunc, expb)
			}()
		}
		return nil
	}
}

func endRTMPStreamHandler(s *LivepeerServer) func(url *url.URL, rtmpStrm stream.RTMPVideoStream) error {
	return func(url *url.URL, rtmpStrm stream.RTMPVideoStream) error {
		rtmpID := rtmpStrm.GetStreamID()
		//Remove RTMP stream
		delete(s.rtmpStreams, core.StreamID(rtmpID))
		if s.CurrentPlaylist != nil {
			s.CurrentPlaylist.EndSession()
			s.CurrentPlaylist = nil
		}

		s.VideoNonceLock.Lock()
		if _, ok := s.VideoNonce[rtmpStrm.GetStreamID()]; ok {
			if monitor.Enabled {
				monitor.LogStreamEndedEvent(s.VideoNonce[rtmpStrm.GetStreamID()])
			}
			delete(s.VideoNonce, rtmpStrm.GetStreamID())
		}
		s.VideoNonceLock.Unlock()
		return nil
	}
}

//End RTMP Publish Handlers

//HLS Play Handlers
func getHLSMasterPlaylistHandler(s *LivepeerServer) func(url *url.URL) (*m3u8.MasterPlaylist, error) {
	return func(url *url.URL) (*m3u8.MasterPlaylist, error) {
		cpl := s.CurrentPlaylist
		if cpl == nil {
			return nil, vidplayer.ErrNotFound
		}
		var manifestID core.ManifestID
		if s.ExposeCurrentManifest && "/stream/current.m3u8" == strings.ToLower(url.Path) {
			manifestID = LastManifestID
		} else {
			var err error
			if manifestID, err = parseManifestID(url.Path); err != nil {
				return nil, vidplayer.ErrNotFound
			}
		}

		if cpl.ManifestID() != manifestID {
			return nil, vidplayer.ErrNotFound
		}
		//Just load it from the cache (it's already hooked up to the network)
		return cpl.GetHLSMasterPlaylist(), nil
	}
}

func getHLSMediaPlaylistHandler(s *LivepeerServer) func(url *url.URL) (*m3u8.MediaPlaylist, error) {
	return func(url *url.URL) (*m3u8.MediaPlaylist, error) {
		strmID, err := parseStreamID(url.Path)
		if err != nil {
			glog.Errorf("Error parsing for stream id: %v", err)
			return nil, err
		}

		if s.CurrentPlaylist == nil {
			return nil, vidplayer.ErrNotFound
		}
		//Get the hls playlist
		pl := s.CurrentPlaylist.GetHLSMediaPlaylist(strmID)
		if pl == nil {
			return nil, vidplayer.ErrNotFound
		}
		return pl, nil
	}
}

func getHLSSegmentHandler(s *LivepeerServer) func(url *url.URL) ([]byte, error) {
	return func(url *url.URL) ([]byte, error) {
		segName := parseSegName(url.Path)
		if segName == "" {
			return nil, vidplayer.ErrNotFound
		}
		if drivers.LocalStorage == nil {
			return nil, vidplayer.ErrNotFound
		}
		strmID, err := parseStreamID(url.Path)
		if err != nil {
			glog.Errorf("Error parsing for stream id: %v", err)
			return nil, vidplayer.ErrNotFound
		}
		mid, _ := strmID.ManifestIDFromStreamID()
		data := drivers.LocalStorage.GetData(string(mid), segName)
		if len(data) > 0 {
			return data, nil
		}
		return nil, vidplayer.ErrNotFound
	}
}

//End HLS Play Handlers

//Start RTMP Play Handlers
func getRTMPStreamHandler(s *LivepeerServer) func(url *url.URL) (stream.RTMPVideoStream, error) {
	return func(url *url.URL) (stream.RTMPVideoStream, error) {
		strmID, err := parseStreamID(url.Path)
		if err != nil {
			glog.Errorf("Error parsing streamID with url %v - %v", url.Path, err)
			return nil, ErrRTMPPlay
		}
		strm, ok := s.rtmpStreams[core.StreamID(strmID)]
		if !ok {
			glog.Errorf("Cannot find RTMP stream")
			return nil, vidplayer.ErrNotFound
		}

		//Could use a subscriber, but not going to here because the RTMP stream doesn't need to be available for consumption by multiple views.  It's only for the segmenter.
		return strm, nil
	}
}

//End RTMP Handlers

//Helper Methods Begin

func parseManifestID(reqPath string) (core.ManifestID, error) {
	var strmID string
	regex, _ := regexp.Compile("\\/stream\\/([[:alpha:]]|\\d)*")
	match := regex.FindString(reqPath)
	if match != "" {
		strmID = strings.Replace(match, "/stream/", "", -1)
	}
	mid := core.ManifestID(strmID)
	if mid.IsValid() {
		return mid, nil
	} else {
		return "", vidplayer.ErrNotFound
	}
}

func parseStreamID(reqPath string) (core.StreamID, error) {
	var strmID string
	regex, _ := regexp.Compile("\\/stream\\/([[:alpha:]]|\\d)*")
	match := regex.FindString(reqPath)
	if match != "" {
		strmID = strings.Replace(match, "/stream/", "", -1)
	}
	sid := core.StreamID(strmID)
	if sid.IsValid() {
		return sid, nil
	} else {
		return "", vidplayer.ErrNotFound
	}
}

func parseSegNameSimple(reqPath string) string {
	parts := strings.Split(reqPath, "/")
	return parts[len(parts)-1]
}

func parseSegName(reqPath string) string {
	var segName string
	regex, _ := regexp.Compile("\\/stream\\/.*\\.ts")
	match := regex.FindString(reqPath)
	if match != "" {
		segName = strings.Replace(match, "/stream/", "", -1)
	}
	return segName
}

func (s *LivepeerServer) GetNodeStatus() *net.NodeStatus {
	// not threadsafe; need to deep copy the playlist
	m := make(map[string]*m3u8.MasterPlaylist, 0)
	if s.CurrentPlaylist != nil {
		m[string(s.CurrentPlaylist.ManifestID())] = s.CurrentPlaylist.GetHLSMasterPlaylist()
	}
	return &net.NodeStatus{Manifests: m}
}
