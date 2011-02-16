#!/usr/bin/env python

# pulls in details for psql server and twitter account from server_details.py, which is assumed to live in the same directory

# usage: ./example.py /path/to/file1 /path/to/file2 ...
import shout
import sys
import string
import time
import igraph
import random
import pg
import re
import traceback
import copy
import warnings
from urllib2 import urlopen
from subprocess import Popen
from genNoms import gatherArtistPic

from server_details import *
from common import *

warnings.simplefilter('ignore', RuntimeWarning)

BUFFER_LENGTH = 4096
GENERATION_TYPE = "both"

TEST = True #boolean to throw a sort of unit test, forces the first playlist to be of length 2.
ERROR_CODES = {shout.SHOUTERR_BUSY : "SHOUTERR_BUSY",
shout.SHOUTERR_CONNECTED : "SHOUTERR_CONNECTED",
shout.SHOUTERR_INSANE : "SHOUTERR_INSANE",
shout.SHOUTERR_MALLOC : "SHOUTERR_MALLOC",
shout.SHOUTERR_METADATA : "SHOUTERR_METADATA",
shout.SHOUTERR_NOCONNECT : "SHOUTERR_NOCONNECT",
shout.SHOUTERR_NOLOGIN : "SHOUTERR_NOLOGIN",
shout.SHOUTERR_SOCKET : "SHOUTERR_SOCKET",
shout.SHOUTERR_SUCCESS : "SHOUTERR_SUCCESS",
shout.SHOUTERR_UNCONNECTED : "SHOUTERR_UNCONNECTED",
shout.SHOUTERR_UNSUPPORTED : "SHOUTERR_UNSUPPORTED",
shout.SHOUT_AI_BITRATE : 'SHOUT_AI_BITRATE',
shout.SHOUT_AI_CHANNELS : 'SHOUT_AI_CHANNELS',
shout.SHOUT_AI_QUALITY : 'SHOUT_AI_QUALITY',
shout.SHOUT_AI_SAMPLERATE : 'SHOUT_AI_SAMPLERATE'}


	
def checkPLCleanliness(graph, tracklist):
	"""checks the tracklist to makesure that all the member tracks have hashkey attributes that are not equal to None.
	if one is found return [] if the list is clean pass it back."""
	for trackname in tracklist:
		if graph.vs.select(track=trackname)[0]['hashkey'] == None:
			return []
	return tracklist
	
	
def main(argv=None):
	if argv==None:
		argv = sys.argv
	if len(argv) < 2 or len(argv) > 3:
		print "Usage: %s songGraph.pkl [vertexDendrogram.pickle]"% argv[0]
		return
	if len(argv) == 3:
		vertClustPkl = argv[2]
	else:
		vertClustPkl = None
	
	s = shout.Shout()
	print "Using libshout version %s" % shout.version()
	
	s.host = 'doc.gold.ac.uk'
	s.port = 8000
	s.user = 'source'
	s.password = 'eac5ovba'
	s.mount = "/vRad.mp3"
	s.name = "Steerable Optimized Self-Organizing Radio"
	s.genre = "CompNets"
	s.url = "http://radio.benfields.net"
	s.format = 'mp3' # | 'vorbis'
	#s.protocol = 'icy' | 'xaudiocast' | 'http'
	# s.public = 0 | 1
	# s.audio_info = { 'key': 'val', ... }
	#  (keys are shout.SHOUT_AI_BITRATE, shout.SHOUT_AI_SAMPLERATE,
	#	shout.SHOUT_AI_CHANNELS, shout.SHOUT_AI_QUALITY)
	currentTracklist = []
	try:
		graphLocation = argv[1]
		loadedGraph = igraph.load(graphLocation, format="pickle")
		ms = copy.deepcopy(loadedGraph)
	except Exception, err:
		print "trouble loading the graph at {0} should point to the graph (as pickle).  Sort it out and try again.".format(argv[1])
		return
	try:
		omras2DB = pg.connect(dbname=DBNAME, host=HOST, user=USER)
	except Exception, err:
		print "trouble connecting to the database.  Sort it out and try again."
		return
	#fetch highest session ID (current session will be this + 1)
	result = omras2DB.query("SELECT max(session_no) FROM sessions").getresult()
	try:
		lastSession = int(result[0][0])
	except:
		print "unable to establish current session number, using -1, assuming sessions table is empty."
		lastSession = -1
	while currentTracklist == []:
		if TEST:
			src = random.randrange(0,len(ms.vs))
			while len(ms.es.select(_source=src))==0:
				src = random.randrange(0,len(ms.vs))
			dst = random.choice(ms.es.select(_source=src)).target
		else:
			src, dst = (random.randrange(0,len(ms.vs)), random.randrange(0,len(ms.vs)))#2 random nodes to start...
		if ms.vs[src]['hashkey'] == None or ms.vs[dst]['hashkey'] == None:
			continue
		currentTracklist = checkPLCleanliness(ms, makePlaylistFromSrcDst(ms, src, dst))
		
	try:
		#clean up any old open sessions
		res = omras2DB.query("UPDATE sessions SET is_current = false WHERE is_current = true")
		for entry in omras2DB.query("SELECT session_no FROM sessions WHERE end_time IS NULL").getresult():
			print "found an open session (number %s), closing now..."%entry[0]
			res = omras2DB.query("UPDATE sessions SET end_time=current_timestamp WHERE session_no = %s"%entry[0])
			try:
				if res != "1":
					print "something screwy happened while trying to close session %s.\n\twrong number of sessions closed."%entry[0]
			except TypeError:
				print "something screwy happened while trying to close session %s.\n\tReturned a non-sequence. Value: %s"%(entry[0], str(res))
	except Exception, err:
		print "ran into an error cleaning up old sessions.\n\tIt may be a good idea to verify the cleanliness of the db.\n\terr:%s"%str(err)
		lastSession = 0

	currentSession = startNewSession(omras2DB, ms, "both", lastSession, "current_timestamp", currentTracklist)
	lastVert = -1
	dst_uid = ms.vs[dst]['hashkey']
	
	try:
		s.open()
		status = s.get_connected()
		if ERROR_CODES.has_key(status):
			print "status: " + str(ERROR_CODES[status])
		else:
			print "status: code unknown::" + str(status)
	except Exception, err:
		print "trouble connecting to the shoutcast server.  Sort it out and try again. Error: %s"%err
		return
		
	exclusionList = "exclusionList.txt"
	eH = open(exclusionList, 'w')
	numSessions = 0
	sessionsStarted = 0
	artist, title = "",""
	try:
		while True:
			res = omras2DB.query("UPDATE sessions SET is_current = true WHERE session_no=%i"%(currentSession))
			sessionTracks = omras2DB.query("SELECT media.path, media.filename, media.uid, playlists.position, playlists.session_no FROM media JOIN playlists ON media.uid = playlists.track_uid AND playlists.session_no = (SELECT session_no FROM sessions WHERE is_current = true) ORDER BY playlists.position ASC").getresult()
			print "%s:: %i tracks in the upcoming playlist"%(argv[0], len(sessionTracks))
			#add all upcoming song except dst to exclusion list (dst gets add on other side of for loop)
			for track in sessionTracks[:-1]:
				eH.write(track[2] + "\n")
			eH.flush()
			if vertClustPkl:
				print "{4} :: launching 'python genNoms.py {0} {1} 9 {2} {3}'".format(graphLocation, exclusionList, dst_uid, vertClustPkl, argv[0])
				runningNom = Popen(['python', "genNoms.py", graphLocation, exclusionList, "9", str(dst_uid), vertClustPkl])
			else:
				print "{3} :: launching 'python genNoms.py {0} {1} 9 {2}'".format(graphLocation, exclusionList, dst_uid, argv[0])
				runningNom = Popen(['python', "genNoms.py", graphLocation, exclusionList, "9", str(dst_uid)])
			sessionsStarted += 1
			print "%s:: nominations being generated..."%argv[0]
			for idx, trackEntry in enumerate(sessionTracks):
				total = 0
				st = time.time()
				thisPath, thisFilename, thisUid, thisPosition, thisSession_no = trackEntry
				if thisSession_no != currentSession:
					print "%s :: WARNING :: sessions are not aligned.  currentSession: %i, thisSession_no: %i"%(argv[0], currentSession, thisSession_no)
				res = omras2DB.query("UPDATE playlists SET isplaying = true WHERE session_no=%s AND track_uid = \'%s\'"%(thisSession_no, thisUid))
				fa = os.path.join(thisPath, thisFilename)
				print "%s:: opening file %i/%i location: %s" %(argv[0], (idx+1),len(sessionTracks), fa)
				try:
					f = open(fa)
				except IOError, err:
					print "%s:: Unable to open file %s.\nReason given: %s.\nMoving on..."%(argv[0], fa, err)
					continue
				except Exception, err:
					print "%s:: something other than an IOError went wrong.\n\tSong: %s\n\tReason given: %s\n\tMoving on..."%(argv[0], fa, err)
					continue
				try:
					artist, msUID, trackNum, title = re.match("(^.*?)_(\d*?)_(\d)(.*?)_lores.mp3$", thisFilename).groups()
				except Exception, err:
					print "%s:: something went wierd spliting the track name (%s). metadata will be odd for a minute.\nError:%s"%(argv[0],fa,err)
				if not lastVert == -1:
					#If we just left a node, remove it.
					print "%s:: deleting vertex %i"%(argv[0],ms.vs.select(hashkey=lastVert)[0].index)
					ms.delete_vertices(ms.vs.select(hashkey=lastVert))
				try:
					lastVert = ms.vs.select(track = fa)[0]['hashkey']
				except IndexError,err:
					lastVert = -1
					print "%s:: trouble derefrencing the upcoming track (%s) for deletion. Moving on..."%(argv[0], fa)
				s.set_metadata({"song":"%s - %s"%(artist, title)})
				if res == "1":
					print "%s:: updated playstatus"%argv[0]
				nbuf = f.read(BUFFER_LENGTH)
				while 1:
					buf = nbuf
					nbuf = f.read(BUFFER_LENGTH)
					total = total + len(buf)
					if len(buf) == 0:
						break
					s.send(buf)
					s.sync()
				f.close()
				et = time.time()
				br = total*0.008/(et-st)
				print "%s:: Sent %d bytes in %d seconds (%f kbps)" % (argv[0],total, et-st, br)
				if (idx == len(sessionTracks) - 2):
					#one song left on the list so we'll stop the voting and calculate the next playlist while the last song plays
					print "{0} :: launching 'python genPlaylist.py {1} {2} {3}'".format(argv[0], graphLocation , exclusionList, dst_uid)
					playlister = Popen(['python', "genPlaylist.py", graphLocation , exclusionList, dst_uid])
	
				res = omras2DB.query("UPDATE playlists SET isplaying = false WHERE session_no=%s AND track_uid = \'%s\'"%(thisSession_no, thisUid))
				if res == "1":
					print "%s:: updated playstatus"%argv[0]
			numSessions += 1
			try:
				eH.write(thisUid + "\n")
				eH.flush()
				dst_uid, sessionNo = omras2DB.query("SELECT dst_track, session_no FROM sessions WHERE session_no = 1+(SELECT session_no FROM sessions WHERE is_current = true)").getresult()[0]
				if (int(sessionNo) != (int(thisSession_no) + 1)):
					print "%s:: WARNING :: sessions don't seem to line up during rollover.  Current session: %s, next session: %s"%(argv[0], str(thisSession_no), str(sessionNo))
				dst = ms.vs.select(hashkey= dst_uid)[0].index
				res = omras2DB.query("UPDATE sessions SET is_current = false WHERE session_no=%i"%currentSession)
				currentSession += 1
			except IndexError:
				print "%s:: the playlister or nominator has fallen over, reinitializing and starting again..."%argv[0]
				######reinitialize the graph
				######should really be a function, but putting off the refactoring for now...
				########################################
				ms = copy.deepcopy(loadedGraph)
				currentTracklist = []
				eH.close()
				eH = open(exclusionList, 'w')
				eH.flush()
				#fetch highest session ID (current session will be this + 1)
				result = omras2DB.query("SELECT max(session_no) FROM sessions").getresult()
				try:
					lastSession = int(result[0][0])
				except:
					print "unable to establish current session number, using -1, assuming sessions table is empty."
					lastSession = -1
				while currentTracklist == []:
					if TEST:
						src = random.randrange(0,len(ms.vs))
						while len(ms.es.select(_source=src))==0:
							src = random.randrange(0,len(ms.vs))
						dst = random.choice(ms.es.select(_source=src)).target
					else:
						src, dst = (random.randrange(0,len(ms.vs)), random.randrange(0,len(ms.vs)))#2 random nodes to start...
					if ms.vs[src]['hashkey'] == None or ms.vs[dst]['hashkey'] == None:
						continue
					currentTracklist = checkPLCleanliness(ms, makePlaylistFromSrcDst(ms, src, dst))
				try:
					#clean up any old open sessions
					res = omras2DB.query("UPDATE sessions SET is_current = false WHERE is_current = true")
					for entry in omras2DB.query("SELECT session_no FROM sessions WHERE end_time IS NULL").getresult():
						print "found an open session (number %s), closing now..."%entry[0]
						res = omras2DB.query("UPDATE sessions SET end_time=current_timestamp WHERE session_no = %s"%entry[0])
						try:
							if res != "1":
								print "something screwy happened while trying to close session %s.\n\twrong number of sessions closed."%entry[0]
						except TypeError:
							print "something screwy happened while trying to close session %s.\n\tReturned a non-sequence. Value: %s"%(entry[0], str(res))
				except Exception, err:
					print "ran into an error cleaning up old sessions.\n\tIt may be a good idea to verify the cleanliness of the db.\n\terr:%s"%str(err)
					lastSession = 0

				currentSession = startNewSession(omras2DB, ms, "both", lastSession, "current_timestamp", currentTracklist)
				lastVert = -1
				dst_uid = ms.vs[dst]['hashkey']

				try:
					s.close()
					s = shout.Shout()
					s.host = 'doc.gold.ac.uk'
					s.port = 8000
					s.user = 'source'
					s.password = 'eac5ovba'
					s.mount = "/vRad.mp3"
					s.name = "Steerable Optimized Self-Organizing Radio"
					s.genre = "CompNets"
					s.url = "http://radio.benfields.net"
					s.format = 'mp3' # | 'vorbis'
					s.open()
					status = s.get_connected()
					if ERROR_CODES.has_key(status):
						print "radio-dameon:: status: " + str(ERROR_CODES[status])
					else:
						print "radio-dameon:: status: code unknown::" + str(status)
				except Exception, err:
					print "trouble connecting to the shoutcast server.  Sort it out and try again. Error: %s"%err
					return
				eH.close()
				exclusionList = "exclusionList.txt"
				eH = open(exclusionList, 'w')
				numSessions = 0
				sessionsStarted = 0
				artist, title = "",""
				continue
				############
				#end reinit	
				############
	
	except KeyboardInterrupt:
		print "%s:: caught the keyboard, finishing up."%(argv[0])
	except Exception:
		print "%s:: caught some sort of error.\nContents of sessionTracks on blowUp:\n%s\nTraceback dump follows:"%(argv[0],str(sessionTracks))
		print "Exception type: %s\nException value: %s\nTraceback:%s"%(str(sys.exc_info()[0]),str(sys.exc_info()[1]),traceback.format_exc())
	finally:
		print "all done!"
		print "timestamping open session:" + str(omras2DB.query("UPDATE sessions SET end_time=current_timestamp WHERE end_time IS NULL;"))
		print "closing sessions: " + str(omras2DB.query("UPDATE sessions SET is_current = false WHERE is_current = true"))
		print "setting all isplaying to false: " + str(omras2DB.query("UPDATE playlists SET isplaying = false WHERE isplaying = true"))
		print "closing icecast stream: " + str(s.close())
		print "closing nominator: "+ str(runningNom.terminate())
		print "closing playlister: "+ str(playlister.terminate())
		print "closing aux files: " + str(eH.close())
		print "started %i sessions."%sessionsStarted
		print "completed %i sessions."%numSessions

if __name__ == '__main__':
	main()
