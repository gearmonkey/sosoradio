#!/usr/bin/env python
# encoding: utf-8
"""
common.py

Some common functions to speed access for song graphs and the psql db

Created by Benjamin Fields on 2009-05-18.
Copyright (c) 2009 Goldsmiths University of London. All rights reserved.
"""

import sys
import os
import os.path
import igraph
import pg
import random
import warnings
from urllib2 import urlopen
from redish.proxy import Proxy

R = Proxy(db=0)

warnings.simplefilter('ignore', RuntimeWarning)
argv = sys.argv

def getTrackList(graph, nodeList, by_hashkey=False):
	trackList = []
	for vert in nodeList:
		if by_hashkey:
			trackList.append(graph.vs.select(hashkey=vert)[0]['track'])
		else:
			trackList.append(graph.vs[vert]['track'])
	return trackList

def avgDelta(graph, nodeList):
	delta = 0
	for idx, node in enumerate(nodeList[1:]):
		delta += igraph.EdgeSeq(graph, [nodeList[idx], node])[0]['marsyasAudioWeight']
	return delta/(len(nodeList)-1) 

def deltaList(graph, nodeList):
	deltaList = []
	for idx, node in enumerate(nodeList[1:]):
		deltaList.append(igraph.EdgeSeq(graph, [nodeList[idx], node])[0]['marsyasAudioWeight'])
	return deltaList
	
def trimString(string2Trim, maxLength, fromFront=0, replaceTrim = '...'):
	"""
	trims strings down to maxLength removing fromFront from the front and any remaing extra from the tail
	"""
	if len(string2Trim) <= maxLength:
		return string2Trim
	if len(string2Trim) - maxLength < fromFront:
		fromFront = len(string2Trim) - maxLength
	string2Trim = string2Trim[fromFront:]
	if string2Trim >= maxLength:return string2Trim
	fromEnd = len(string2Trim) - maxLength - fromFront - len(replaceTrim)
	string2Trim = string2Trim[-fromEnd:]
	return string2Trim + replaceTrim
	
def makePlaylistFromSrcDst(songGraph, src, dst):
	return getTrackList(songGraph, songGraph.get_shortest_paths(src, weights='marsyasAudioWeight')[dst])

def insertPlaylist(db, songGraph, currentSession, trackList, by_hashkey=False):
	for idx, track in enumerate(trackList):
		if by_hashkey:
			res = db.query("INSERT INTO playlists (session_no, track_uid, position) VALUES (%i, \'%s\', %i)"%(currentSession, 
			track,idx+1))
		else:
			vert = songGraph.vs.select(track=track)[0]
			res = db.query("INSERT INTO playlists (session_no, track_uid, position) VALUES (%i, \'%s\', %i)"%(currentSession, 
			songGraph.vs.select(track=track)[0]['hashkey'],idx+1))
		#errorcheck
		print "insert returned: " + str(res)
	
def startNewSession(db, songGraph, genType, currentSession, startTime, trackList=None, by_hashkey=False):
	"""Note that if you don't give a trackList the playlist for the session will need to be inserted a another point."""
	if trackList and by_hashkey:
		res = db.query("INSERT INTO sessions (start_time, src_track, dst_track, num_tracks, gen_type) VALUES (%s, \'%s\', \'%s\', %i, \'%s\')\
		RETURNING session_no"%(startTime, trackList[0],trackList[-1],len(trackList), genType)).getresult()
	elif trackList and not by_hashkey:
		res = db.query("INSERT INTO sessions (start_time, src_track, dst_track, num_tracks, gen_type) VALUES (%s, \'%s\', \'%s\', %i, \'%s\')\
		RETURNING session_no"%(startTime, songGraph.vs.select(track=trackList[0])[0]['hashkey'],songGraph.vs.select(track=trackList[-1])[0]['hashkey'],
		len(trackList), genType)).getresult()
	else:
		res = db.query("INSERT INTO sessions (start_time, num_tracks, gen_type) VALUES (%s, 0, \'%s\')\
		RETURNING session_no"%(startTime, genType)).getresult()		
	try:
		if len(res) != 1:
				print "something screwy happened while trying to start session %s.\n\twrong number of sessions opened."%currentSession + 1
	except TypeError:
		print "something screwy happened while trying to start session %is.\n\tReturned a non-sequence. Value: %s"%(currentSession + 1, str(res))
	if res[0][0] != currentSession+1:
		print "something screwy happened while trying to start session %i.\n\twrong session number was generated. Value: %s.\n\tUsing this new value."%(currentSession+1, res[0][0])
	currentSession = res[0][0]
	if trackList: insertPlaylist(db, songGraph, currentSession, trackList, by_hashkey)
	return currentSession

def findNextSessionDst(db):
	"""find the next Session's dst by tallying this session's votes."""
	currentSession = db.query("SELECT session_no FROM sessions WHERE is_current = true").getresult()[0][0]
	candidates = db.query("SELECT DISTINCT track_uid  FROM votes WHERE session_no=%s"%currentSession).getresult()
	if candidates == []:
		#no one voted so pick a nominee at random and return it
		try:
			noms = R["sessionNo:"+str(currentSession)+":nominees"]
			try:
				nominee = random.choice(noms)
				nominee = db.query("SELECT uid, path, filename FROM media WHERE uid = '%s'"%random.choice(noms)).getresult()[0]		
			except IndexError, err:
				print "%s :: couldn't select a winner from no vote pool, reason: %s \nquery returned: %s"%(argv[0], str(err), str(noms))
				return
		except KeyError:
			noms = db.query("SELECT nominees.uid, media.path, media.filename FROM nominees JOIN media ON media.uid = nominees.uid WHERE nominees.session_no = %s"%currentSession).getresult()
			try:
				nominee = random.choice(noms)
			except IndexError, err:
				print "%s :: couldn't select a winner from no vote pool, reason: %s \nquery returned: %s"%(argv[0], str(err), str(noms))
				return
		try:
			completePath = os.path.join(nominee[1],nominee[2])
			print "%s :: incoming path: %s"%(argv[0],completePath)
		except:
			print "%s :: couldn't generate path for nominee\nquery returned: %s"%(argv[0], str(nominee))
			completePath = ""
		return (nominee[0],completePath)
	winner = [0, (-1,-1)]#[numVotes,(uid, path)] in event of tie first seen candidate is declared winner
	for nominee in candidates:
		numVotes = db.query("SELECT COUNT (ipaddr) FROM votes WHERE track_uid='%s' AND session_no=%s"%(nominee[0],currentSession)).getresult()						   
		if numVotes[0][0] > winner[0]:																																	  
			winner[0] = numVotes[0][0]
			path = db.query("SELECT path, filename FROM media WHERE uid='%s'"%nominee[0]).getresult()	
			try:
				completePath = os.path.join(path[0][0],path[0][1])
				print "%s :: incoming path: %s"%(argv[0],completePath)
			except:
				print "%s :: couldn't generate path for uid %s\nquery returned: %s"%(argv[0], nominee[0], str(path))
				completePath = ""																											   
			winner[1] = (nominee[0],completePath)	
	return winner[1]
	
def insertNewListandStartNewSession(db, songGraph=None, trackList=None, genType='both', by_hashkey=False):
	currentSession = db.query("SELECT session_no FROM sessions WHERE is_current = true").getresult()[0][0]
	#close old session
	res = db.query("UPDATE sessions SET end_time=current_timestamp WHERE end_time IS NULL RETURNING end_time").getresult()
	try:
		if len(res) != 1:
			print "something screwy happened while trying to close session %s.\n\twrong number of sessions closed."%currentSession
	except TypeError:
		print "something screwy happened while trying to close session %s.\n\tReturned a non-sequence. Value: %s"%(currentSession, str(res))
	#start new session
	startNewSession(db, songGraph, genType, currentSession, "current_timestamp", trackList, by_hashkey)

	
def main():
	pass


if __name__ == '__main__':
	main()

