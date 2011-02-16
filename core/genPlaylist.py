#!/usr/bin/env python
# encoding: utf-8
"""
genPlaylist.py

Usage: genPlaylist.py songGraph.pickle exclusionList.txt src

generates a playlist between src and dst using the graph contained in songGraph.pickle ignoring the verts with hashkeys in exclusionList.txt
the new playlist is then added to the psql db and a new session is created.
src should be a hashkey.
Dst is determined by tallying votes 


pulls in details for psql server and twitter account from server_details.py, which is assumed to live in the same directory

Created by Ben Fields on 2009-12-15.
Copyright (c) 2009 Goldsmiths University of London. All rights reserved.
"""

import sys
import os
import pg
import random
import igraph
import shutil
import warnings
import twitter, time
from redish.proxy import Proxy
from common import *
from server_details import *

GENERATION_TYPE = 'both'

warnings.simplefilter('ignore', RuntimeWarning)

R = Proxy(db=0)


def main(argv=None):
	if argv==None:
		argv = sys.argv
	if len(argv) != 4:
		print "Usage: %s songGraph.pickle exclusionList.txt src"% argv[0]
		return
	
	src = argv[3]
	print "%s:: connecting to database..."%argv[0]
	try:
		omras2DB = pg.connect(dbname=DBNAME, host=HOST, user=USER)
	except Exception, err:
		print "trouble connecting to the database.  Sort it out and try again."
		return
	
	print "%s:: creating next playlist..."%argv[0]
	dstUID, dstPath = findNextSessionDst(omras2DB)
	print "%s:: next dst, path:%s uid:%s "%(argv[0], dstPath, dstUID),
	try:
		try:
			currentTracklist = R["src_dst:"+str(src)+ '_'+str(dstUID)+":path"] 
			print "%s:: inserting new list of length %i..."%(argv[0], len(currentTracklist))
			insertNewListandStartNewSession(omras2DB, None, currentTracklist, GENERATION_TYPE, by_hashkey=True)
		except KeyError:
			g = igraph.load(argv[1])
			exclusionRaw = open(argv[2]).readlines()
			exclusionList = []
			for edge in exclusionRaw:
				exclusionList.append(edge.strip())
			print "%s:: removed %i vertices from graph."%(argv[0], len(exclusionList))
			g = g.delete_vertices(g.vs.select(hashkey_in=exclusionList))
			print "%s:: removed %i vertices from graph due to unknown duration."%(argv[0], len(g.vs.select(duration=None)))
			g = g.delete_vertices(g.vs.select(duration=None))
			print "%s:: removed %i vertices from graph due to unknown hashkey."%(argv[0], len(g.vs.select(hashkey=None)))
			g = g.delete_vertices(g.vs.select(hashkey=None))
			
			dst_vert = g.vs.select(track=dstPath)[0]
			dst = dst_vert.index
			print "%s:: with index "%argv[0], str(dst)
			
			currentTracklist = makePlaylistFromSrcDst(g, g.vs.select(hashkey=src)[0].index, dst)[1:]
			print "%s:: inserting new list of length %i..."%(argv[0], len(currentTracklist))
			insertNewListandStartNewSession(omras2DB, g, currentTracklist, GENERATION_TYPE)
	except pg.ProgrammingError, err:
		print "{0} :: ERROR :: There was a db error.\n\tmsg: {1}\n\tsrc: {2}, dst: {3}\n\ttracklist: {4}\n\texclusionList is now in: {5}".format(argv[0], err, src, dstUID, currentTracklist, argv[2][:-4] + "_"+str(src) + '_' + str(dstUID) + '.txt')
		shutil.copy(argv[2], argv[2][:-4] + "_"+str(src) + '_' + str(dstUID) + '.txt')
	me = twitter.Api(username=TWITTER_USER, password=TWITTER_PASS)
	me.PostUpdate("Upcoming tracks on SoSoRadio starting at about {0}, http://radio.benfields.net :".format(time.asctime()))
	runningTime = 0
	for uid in R["src_dst:"+str(src)+ '_'+str(dstUID)+":path"]:
		artist, title, dur = omras2DB.query("SELECT artist, track, duration FROM media WHERE uid = \'{0}\'".format(uid)).getresult()[0]
		try:
			minutes = int(dur[0])
			if int(dur[2:4]) > 30:
				minutes += 1
		except:
			minutes = 3
		if runningTime == 0:
			me.PostUpdate("Next: \'" + trimString(artist, 9) + "\' - \'" + trimString(title, 15) + "\' ")
			runningTime += 3
		else:
			me.PostUpdate("In {0} min : \'".format(runningTime) + trimString(artist, 9) + "\' - \'" + trimString(title, 15) + "\' ")
		runningTime = runningTime + minutes
			
	# me.PostUpdates(statusMsg, continuation=u'\u2026')
		
		
	


if __name__ == '__main__':
	main()

