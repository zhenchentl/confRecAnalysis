#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-01-10 20:03:53
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import sys
import networkx as nx
from Redis.redisHelper import RedisHelper
from utils.params import *
import random
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class BaconNumber(object):
    """docstring for BaconNumber"""
    def __init__(self):
        self.G = nx.Graph()
        self.mRedis = RedisHelper()
        self.buildGraph()

    def buildGraph(self):
        authors = self.mRedis.getAllAuthors()
        index = 0
        for author in authors:
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            coaus = self.mRedis.getAuCoauthors(author)
            confs = self.mRedis.getAuConfs(author)
            for coau in coaus:
                self.G.add_edge(author, coau)
            for conf in confs:
                self.G.add_edge(author, conf)

    def getGraphNodeCount(self):
        return len(self.G.nodes())

    def getGraphEdgeCount(self):
        return len(self.G.edges())

    def shortestPath(self, s, t):
        return nx.shortest_path_length(self.G, s, t)

def extracStarsAndTargets():
    mRedis = RedisHelper()
    starts = dict()
    targets = dict()
    authors = mRedis.getAllAuthors()
    confNumAuthors = dict()
    index = 0
    for author in authors:
        index += 1
        if index % 1000 == 0:
            logging.info(index)
        confNum = len(mRedis.getAuConfs(author))
        tmp = confNumAuthors.setdefault(confNum, [])
        tmp.append(author)
    logging.info('-------')
    for i in range(1, 101):
        logging.info(i)
        confauhtors = confNumAuthors[i]
        count = 0
        while count < 10:
            au = random.choice(confauhtors)
            if au not in targets.keys():
                targets[au] = i
                count += 1
    count = 0
    while count < 100:
        author = random.choice(authors)
        confNum = len(mRedis.getAuConfs(author))
        if author not in starts and confNum > 100:
            starts[author] = confNum
            count += 1
            logging.info(count)
    authors = []
    confNumAuthors = {}

    with open(OUTPUT_STAR_AUTHORS, 'w') as fileWriter:
        for author, confNum in starts.items():
            fileWriter.write(author + '\t' + str(confNum) + '\n')
    fileWriter.close()
    with open(OUTPUT_TARGET_AUTHORS, 'w') as fileWriter:
        for author, confNum in targets.items():
            fileWriter.write(author + '\t' + str(confNum) + '\n')
    fileWriter.close()

if __name__ == '__main__':
    # baconNum = BaconNumber()
    # logging.info(baconNum.shortestPath("npl", "jcam"))
    extracStarsAndTargets()
