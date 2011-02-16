#!/usr/bin/env python
# encoding: utf-8
"""
genNoms.py

Usage: genNoms songGraph.pickle exclusionList NumNoms src [commStruct]

generates nominees for the round after the current one and inserts them into the omras2DB.
If currentSession+1 does not exist it will be created.
this program will fail if no currentSession.
	Will also attempt to find valid uris for an artist pic with each nominee, this uri will be tab
	seperated on the same line as each associated uid
exclusionList is a file with a list of the hashkeys of all vertices to be ingored for determining paths, one per line.
The generation method will be a slightly to be determined method based on the commStuct passed in.
src should either be None or the uid for the upcoming src.
if src s None then the nominees will be generated without awareness of the start.
If no commStruct is passed in NumNoms songs will be randomly selected and used.
After being generated the new nominee list will be archived locally and ftped to the server.

pulls in details for psql server and twitter account from server_details.py, which is assumed to live in the same directory

Created by Benjamin Fields on 2009-12-03.
Copyright (c) 2009 Goldsmith University of London.
"""

import sys
import os, os.path
import igraph
import cPickle
import time
import pg
import re
import urllib2
import copy
import warnings
from common import *
from server_details import *

from random import randrange, sample
from redish.proxy import Proxy

warnings.simplefilter('ignore', RuntimeWarning)
FILENAME = "currentNominees.txt" #remote copy name, local copy name wil have timestamp attached.

CUTHEIGHT = 80	 #how many clusters
IDEALTIME = 1500 #length of 'ideal' playlist in second
GIVE = 0.2 #percentage away (plus or minus) from the ideal that is allowed for playtime

GENTYPE = 'both' #for session insertion
MODE = 'marsyasAudioWeight' #|'audioWeight'  for old gmmemd distance| None for social only selection

#terrible hardcoded parameters...
UPLOAD = True #set to False to bypass upload (for testing)
DEBUG = False
EXPAND = False #True will perform cluster expansion, very slow (2.5x runtime)
SKIPSINGLES = True #If true clusters with only one member will be ignored

catalogue = "myspace"
artistPage="http://profile.myspace.com/index.cfm?fuseaction=user.viewprofile&friendid=%s"

R = Proxy(db=0)
	

def gatherArtistPic(hashKey):
	"""	returns the uri of the current artist pic for the track associated with hashKey."""
	omras2DB = pg.connect(dbname=DBNAME, host=HOST, user=USER)
	result = omras2DB.query("""SELECT artist, track, comments FROM media WHERE catalogue LIKE \'%%%s%%\' and uid=\'%s\'"""%(str(catalogue),hashKey)).getresult()
	omras2DB.close()
	try:
		uri = artistPage%result[0][2].split("->")[-1]
	except:
		#if all else fails use the blank pic and move on
		print "trouble creating the artist page uri for hashKey: %s\nresult back was: %s"%(hashKey, str(result))
		return "images/noPic.jpg"
		
	try:
		resp = urllib2.urlopen(uri)
		content = resp.read()
		return re.findall(r"friendID=\d*\"><img src=\".*?\" ",content)[0].split("<img src=\"")[1].rstrip('\" ')
	except:
		print "trouble creating the artist image uri for hashKey: %s\nresult back was: %s"%(hashKey, str(result))
		return "images/noPic.jpg"

def getTime(graph, nodeList):
	length = 0
	for idx in nodeList:
		try:
			length += graph.vs[idx]['duration']
		except Exception, err:
			print "trying to add duration for this list:\n%s\ndid not work out for this reason:%s"%(str(nodeList),str(err))
			return -1
	return length

def expandAndResetContenders(bottomCap, topCap, ranks, rankCopy):
	"""returns a widened bottomCap and topCap and recalculates the contenders based on this.  If the bottom and topCaps are already at maximum width, then returns None.  Returns bottomCap, topCap, contenders upon success"""
	if not EXPAND:return None #override the expansion...
	if bottomCap == 0:
		print "no nodes in this cluster close enough to use as a nominee."
		return
	if DEBUG: print "expanding boundaries from {0} and {1} to ".format(topCap, bottomCap),
	if bottomCap == int((len(ranks) - 1) * 0.25):
		bottomCap = int((len(ranks) - 1) * 0.5)
	elif bottomCap == int((len(ranks) - 1) * 0.5):
		bottomCap = int((len(ranks) - 1) * 0.75)
	elif bottomCap != len(ranks):
		bottomCap = len(ranks)
	else:
		return None
	topCap = 0
	if DEBUG: print "{0} and {1}.".format(topCap, bottomCap)
	idx = 0
	return bottomCap, topCap, idx, sample(rankCopy[-bottomCap:],len(rankCopy[-bottomCap:]))

def main(argv=None):
	if argv==None:
		argv = sys.argv
	if len(argv) < 5 or len(argv) > 6:
		print "Usage: %s songGraph.pickle exclusionList NumNoms src [commStruct]"% argv[0]
		return
	global GIVE
	exclusionRaw = open(argv[2]).readlines()
	exclusionList = []
	for edge in exclusionRaw:
		exclusionList.append(edge.strip())
	numNoms = int(argv[3])
	
	if len(argv) == 6:
		# songReport = cPickle.load(open(argv[5]))
		# commStruct = songReport.vcG
		commStruct = cPickle.load(open(argv[5]))
		g = commStruct.graph.copy()
	else:
		commStruct = None
		g = igraph.load(argv[1])

	if argv[4] == 'None':
		src = None
		paths = None
	else:
		src = argv[4]
	
	
	omras2DB = pg.connect(dbname=DBNAME, host=HOST, user=USER)
	try:
		currentSession = omras2DB.query("SELECT session_no FROM sessions WHERE is_current = true").getresult()[0][0]
		nomSession = int(currentSession)
		print "%s :: Nominations will be inserted into session %i"%(argv[0], nomSession)
	except IndexError:
		print "%s :: no current session. turn on the radio dameon and try again."%argv[0]
		return
	
	print "%s:: removed %i edges from graph."%(argv[0], len(exclusionList))
	g = g.delete_vertices(g.vs.select(hashkey_in=exclusionList))

	toDelete = g.vs.select(duration=None)
	print "%s:: removed %i vertices from graph due to unknown duration."%(argv[0], len(toDelete))
	g = g.delete_vertices(toDelete)

	toDelete = g.vs.select(hashkey=None)
	print "%s:: removed %i vertices from graph due to unknown hashkey."%(argv[0], len(toDelete))
	g = g.delete_vertices(toDelete)
	
	deadEnds = g.vs.select(_outdegree=0)

	
	
	try:
		src = g.vs.select(hashkey= src)[0].index
	except Exception, err:
		print "%s :: ERROR :: trouble dereferencing src uid, nominees may be incorrectly filtered. \n\tmsg:%s\n\tproceeding w/o src"%(argv[0], str(err))
		src = None
	if src in map(lambda x:x.index, deadEnds):
		print "src lacks outlets, proceeding without src."
		src = None
	
	if not src == None:
		print "%s :: using %s as a src for nomination generation."%(argv[0], g.vs[src]['track'])
		paths = g.get_shortest_paths(src, MODE)
	else:
		paths = None
	seenNodes = map(lambda x:x.index, deadEnds) #seed the skip list with all the nodes w/o outlets
	if not commStruct:
		nomSequence = sample(g.vs, len(g.vs))
		idx=0
		for i in range(numNoms):
			possibleNom = nomSequence[idx]
			while possibleNom.index in seenNodes:
				try:
					idx += 1
					possibleNom = nomSequence[idx]
				except IndexError:
					print "%s :: got to the end of the random sequence from the seenNode check, rolling over and increasing GIVE"%argv[0]
					print "%s :: Nominees left to select: %i, current value of give: %f, new value of give: "%(argv[0],numNoms-i, GIVE),
					GIVE += ((float(numNoms)-i)/20)
					print str(GIVE)
					idx = 0
			if paths:
				playlength = getTime(g, paths[possibleNom.index][1:])#don't include the src since it was played as the dst before
				#playtime check, with an error allowance of +/- %20
				while playlength < (IDEALTIME * (1.0 - GIVE)) or playlength > (IDEALTIME * (1.0 + GIVE)):
					#growing GIVE can lead to a negative playlist time being allowed, which is weird...
					try:
						idx +=1
						possibleNom = nomSequence[idx]
						while possibleNom.index in seenNodes:
							idx +=1
							possibleNom = nomSequence[idx]
					except IndexError:
						print "%s :: got to the end of the random sequence while checking playlist length, rolling over and increasing GIVE"%argv[0]
						print "%s :: Nominees left to select: %i, current value of give: %f, new value of give: "%(argv[0],numNoms-i, GIVE),
						GIVE += ((float(numNoms)-i)/20)
						if GIVE >= 1.0 : GIVE = 0.999
						print str(GIVE)
						idx = 0

					playlength = getTime(g, paths[possibleNom.index][1:])#don't include the src since it was played as the dst before
			seenNodes.append(possibleNom.index)
			omras2DB.query("INSERT INTO nominees (session_no, uid, picuri) VALUES (%i, \'%s\', \'%s\')"%(nomSession, str(possibleNom['hashkey']), gatherArtistPic(possibleNom['hashkey'])))
			try:
				R["sessionNo:"+str(nomSession)+":nominees"] = list(R["sessionNo:"+str(nomSession)+":nominees"]) + [possibleNom['hashkey']]
			except KeyError:
				R["sessionNo:"+str(nomSession)+":nominees"] = [possibleNom['hashkey']]
			R["src_dst:"+str(g.vs[src]['hashkey'])+ '_'+str(possibleNom['hashkey'])+":path"] = map(lambda g,x:g.vs[x]['hashkey'], paths[possibleNom.index][1:])
		omras2DB.close()
	else:
		clusters = igraph.VertexClustering(commStruct.graph,membership=commStruct.cut(CUTHEIGHT))
		candidates = []
		
		for clustNum in sample(range(len(clusters)), len(clusters)):
			if SKIPSINGLES and (len(clusters[clustNum]) == 1):
				if DEBUG: print "{0} :: skipping cluster {1} as length is 1.".format(argv[0], clustNum)
				continue
			subG = clusters.subgraph(clustNum)
			if DEBUG: print "{0} :: on cluster {1} of {2}.  This cluster has {3} nodes in it.".format(argv[0], clustNum, CUTHEIGHT, len(subG.vs))
			give = int(GIVE)
			if src:
				if len(subG.vs.select(hashkey=g.vs[src]['hashkey'])) > 0:
					#don't want a nominee from the cluster we're going to end in
					continue
			try:
				ranks = subG.pagerank(weights=MODE)
			except igraph.core.InternalError, err:
				print "%s:: ERROR :: cluster does not have enough members for centrality. Number of members: %i"%(argv[0], len(subG.vs))
				try:
					if len(subG.vs) == 1:
						fullIndex = g.vs.select(hashkey=subG.vs[0]['hashkey'])[0].index
						seenNodes.append(fullIndex)
						playlength = getTime(g, paths[fullIndex][1:]) 
						if playlength > (IDEALTIME * (1.0 - give)) and playlength < (IDEALTIME * (1.0 + give)):
							candidates.append(fullIndex)
							print "%s :: added cluster's lone member to candidate list as path was an acceptable dist from src"%argv[0]
						else: print "%s :: didn't add cluster's lone member to candidate list as path was unacceptable dist from src"%argv[0]
				except Exception, err:
					print "%s :: unable to add cluster's lone member to candidate list (or some other error)\n\terror given: %s"%(argv[0], str(err))
				continue
			
			#we're selecting from top 15 - 5 percent of ranked songs to move out of the head
			topCap = int((len(ranks) - 1) * 0.02)
			bottomCap = int((len(ranks) - 1) * 0.20)
			rankCopy = list(ranks)
			rankCopy.sort()
			
			playlength = 0
			idx = 0
			if topCap == 0:
				contenders = sample(rankCopy[-bottomCap:],len(rankCopy[-bottomCap:]))
			else:
				contenders = sample(rankCopy[-bottomCap:-topCap],len(rankCopy[-bottomCap:-topCap]))
			# print "contenders: {0}\n85:{1}  95:{2}\nranks:{3}\nrankCopy:{4}\nsliced rankCopy: {5}".format(contenders,bottomCap, topCap, ranks, rankCopy, rankCopy[-bottomCap:-topCap])
			possibleNom = ranks.index(contenders[idx])
			fullIndex = None
			if not paths:
				playlength = None
				while not fullIndex:
					try:
						fullIndex = g.vs.select(hashkey=subG.vs[possibleNom]['hashkey'])[0].index
						seenNodes.append(fullIndex)
						candidates.append(fullIndex)
					except IndexError, err:
						if subG.vs[possibleNom]['hashkey'] != None:
							print "found a node id mismatch for %i from cluster %i while trying to find nominees.  \
							\n\tError msg: %s\n\tCluster size:%i, Graph size:%i, hashkey:%s"%(possibleNom, clustNum, err, len(subG.vs), 
							len(g.vs), subG.vs[possibleNom]['hashkey'])
						idx += 1
						if idx >= len(contenders):
							try:
								bottomCap, topCap, idx, contenders = expandAndResetContenders(bottomCap, topCap, ranks, rankCopy)
							except TypeError:
								playlength = None
								break
						possibleNom = ranks.index(contenders[idx])
						continue
				continue
			while playlength < (IDEALTIME * (1.0 - give)) or playlength > (IDEALTIME * (1.0 + give)):
				try:
					fullIndex = g.vs.select(track=subG.vs[possibleNom]['track'])[0].index
					seenNodes.append(fullIndex)
				except IndexError, err:
					if subG.vs[possibleNom]['hashkey'] != None:
						print "found a node id mismatch for %i from cluster %i while trying to find nominees.  \
						\n\tError msg: %s\n\tCluster size:%i, Graph size:%i, hashkey:%s"%(possibleNom, clustNum, err, len(subG.vs), 
						len(g.vs), subG.vs[possibleNom]['hashkey'])
					idx += 1
					if idx >= len(contenders):
						try:
							bottomCap, topCap, idx, contenders = expandAndResetContenders(bottomCap, topCap, ranks, rankCopy)
						except TypeError:
							playlength = None
							break
					possibleNom = ranks.index(contenders[idx])
					continue
				playlength = getTime(g, paths[fullIndex][1:])
				idx += 1
				if idx >= len(contenders):
					try:
						bottomCap, topCap, idx, contenders = expandAndResetContenders(bottomCap, topCap, ranks, rankCopy)
					except TypeError:
						playlength = None
						break
				possibleNom = ranks.index(contenders[idx])
			if not playlength == None:candidates.append(fullIndex)
			if len(candidates) >= numNoms:
				print "{0} :: found enough candidates...".format(argv[0])
				break
		try:
			print "{0} :: Out of {1} clusters, {2} provided nominees with acceptable playlist lengths.".format(argv[0], CUTHEIGHT, len(candidates))
			for nom in sample(candidates, numNoms):
				omras2DB.query("INSERT INTO nominees (session_no, uid, picuri) VALUES (%i, \'%s\', \'%s\')"%(nomSession, str(g.vs[nom]['hashkey']), gatherArtistPic(g.vs[nom]['hashkey'])))
				try:
					R["sessionNo:"+str(nomSession)+":nominees"] = list(R["sessionNo:"+str(nomSession)+":nominees"]) + [g.vs[nom]['hashkey']]
				except KeyError:
					R["sessionNo:"+str(nomSession)+":nominees"] = [g.vs[nom]['hashkey']]
				if paths:
					R["src_dst:"+str(g.vs[src]['hashkey'])+ '_'+str(g.vs[nom]['hashkey'])+":path"] = map(lambda x:g.vs[x]['hashkey'], paths[nom][1:])
				else:
					try:
						paths = g.get_shortest_paths(sample(g.vs.select(_outdegree_gt=0, hashkey_ne=g.vs[nom]['hashkey']),1)[0].index, MODE)
						R["src_dst:NoSrc_"+str(g.vs[nom]['hashkey'])+":path"] = map(lambda x:g.vs[x]['hashkey'], paths[nom][1:])
						paths = None
					except Exception, err:
						print "blow up: " + str(Exception) + "::" + str(err)
						return
		except ValueError:
			print "%s ::There are only %i clusters with possible nominees, which is less than the desired number on nominees of %i so all possibles will be used. and augmented with random selections."%(argv[0], len(candidates), numNoms)
			for nom in candidates:
				omras2DB.query("INSERT INTO nominees (session_no, uid, picuri) VALUES (%i, \'%s\', \'%s\')"%(nomSession, str(g.vs[nom]['hashkey']), gatherArtistPic(g.vs[nom]['hashkey'])))
				try:
					R["sessionNo:"+str(nomSession)+":nominees"] = list(R["sessionNo:"+str(nomSession)+":nominees"]) + [g.vs[nom]['hashkey']]
				except KeyError:
					R["sessionNo:"+str(nomSession)+":nominees"] = [g.vs[nom]['hashkey']]
				R["src_dst:"+str(g.vs[src]['hashkey'])+ '_'+str(g.vs[nom]['hashkey'])+":path"] = map(lambda x:g.vs[x]['hashkey'], paths[nom][1:])
			nomSequence = sample(g.vs, len(g.vs))
			idx=0
			for i in range(numNoms - len(candidates)):	
				possibleNom = nomSequence[idx]
				while possibleNom.index in seenNodes:
					try:
						idx += 1
						possibleNom = nomSequence[idx]
					except IndexError:
						print "%s :: got to the end of the random sequence from the seenNode check, rolling over and increasing GIVE"%argv[0]
						print "%s :: Nominees left to select: %i, current value of give: %f, new value of give: "%(argv[0],numNoms-i, GIVE),
						GIVE += ((float(numNoms)-i)/20)
						if GIVE >= 0.99:
							print str(GIVE) + "\n\tnominator is stuck in the loop. failing."
							break
						if GIVE >= 1.0 : GIVE = 0.999
						print str(GIVE)
						idx = 0
				if paths:
					playlength = getTime(g, paths[possibleNom.index][1:])#don't include the src since it was played as the dst before
					#playtime check, with an error allowance of +/- %20
					while playlength < (IDEALTIME * (1.0 - GIVE)) or playlength > (IDEALTIME * (1.0 + GIVE)):
						#growing GIVE can lead to a negative playlist time being allowed, which is weird...
						try:
							idx +=1
							possibleNom = nomSequence[idx]
							while possibleNom.index in seenNodes:
								idx +=1
								possibleNom = nomSequence[idx]
						except IndexError:
							print "%s :: got to the end of the random sequence while checking playlist length, rolling over and increasing GIVE"%argv[0]
							print "%s :: Nominees left to select: %i, current value of give: %f, new value of give: "%(argv[0],numNoms-i, GIVE),
							GIVE += ((float(numNoms)-i)/20)
							if GIVE >= 0.99:
								print str(GIVE) + "\n\tnominator is stuck in the loop. failing."
								break
							if GIVE >= 1.0 : GIVE = 0.999
							print str(GIVE)
							
							idx = 0

						playlength = getTime(g, paths[possibleNom.index][1:])#don't include the src since it was played as the dst before
				seenNodes.append(possibleNom.index)
				omras2DB.query("INSERT INTO nominees (session_no, uid, picuri) VALUES (%i, \'%s\', \'%s\')"%(nomSession, str(possibleNom['hashkey']), gatherArtistPic(possibleNom['hashkey'])))
				try:
					R["sessionNo:"+str(nomSession)+":nominees"] = list(R["sessionNo:"+str(nomSession)+":nominees"]) + [possibleNom['hashkey']]
				except KeyError:
					R["sessionNo:"+str(nomSession)+":nominees"] = [possibleNom['hashkey']]
				print "{0} :: added {1} to nom list. fullpath is :\n{2}".format(argv[0], possibleNom['hashkey'], map(lambda x:'\t'+str(g.vs[x]['hashkey'])+'\n', paths[possibleNom.index]))
				R["src_dst:"+str(g.vs[src]['hashkey'])+ '_'+str(possibleNom['hashkey'])+":path"] = map(lambda x:g.vs[x]['hashkey'], paths[possibleNom.index][1:])
			omras2DB.close()
	print "%s :: nominees have been generated and inserted for session %i"%(argv[0], nomSession)
			
			
		
	


if __name__ == '__main__':
	main()

