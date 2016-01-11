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
        self.stars = dict()
        self.targets = dict()
        self.loadStarsAndTargets()
        logging.info('loadStarsAndTargets done---------------')
        self.shortestPathLength = dict()
        self.mRedis = RedisHelper()
        self.buildGraph()
        logging.info('---------------')

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

    def getShortestPathLength(self):
        self.targets = dict(sorted(self.targets.iteritems(), key = lambda d:d[1]))
        index = 0
        for author, confNum in self.targets.items():
            for star in self.stars.keys():
                try:
                    length = self.shortestPath(author, star)
                except:
                    length = -1
                tmp = self.shortestPathLength.setdefault(author, [])
                tmp.append(length)
                index += 1
                logging.info(str(index) + '---' + str(length))
        with open(OUTPUT_AUTHORS_BACOM_NUM, 'w') as fileWriter:
            nodeCount = self.getGraphNodeCount()
            edgesCount = self.getGraphEdgeCount()
            fileWriter.write('nodes:' + str(nodeCount) + '\t' + 'edges:' + str(edgesCount) + '\n')
            logging.info('nodes:' + str(nodeCount) + '\t' + 'edges:' + str(edgesCount) + '\n')
            for author, bacons in self.shortestPathLength.items():
                baconStr = ''
                count, sumB, avg = 0, 0.0, 0.0
                for bacon in bacons:
                    baconStr += str(bacon) + '\t'
                    if bacon > 0 and bacon < 10000:
                        sumB += bacon
                        count += 1
                avg = 0 if count == 0 else sumB * 1.0 / count
                sb = author + '\t' + str(self.targets[author]) + '\t' + str(avg) + '\t' + baconStr + '\n'
                fileWriter.write(sb)
        fileWriter.close()
        self.shortestPathLength = {}
        self.G = None

    def loadStarsAndTargets(self):
        with open(OUTPUT_STAR_AUTHORS) as fileReader:
            for line in fileReader:
                star = line.split('\t')[0]
                confNum = line.split('\t')[1]
                self.stars[star] = confNum
        fileReader.close()
        with open(OUTPUT_TARGET_AUTHORS) as fileReader:
            for line in fileReader:
                target = line.split('\t')[0]
                confNum = line.split('\t')[1]
                self.targets[target] = confNum
        fileReader.close()

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
        if author not in stars and confNum > 100:
            stars[author] = confNum
            count += 1
            logging.info(count)
    authors = []
    confNumAuthors = {}

    with open(OUTPUT_STAR_AUTHORS, 'w') as fileWriter:
        for author, confNum in stars.items():
            fileWriter.write(author + '\t' + str(confNum) + '\n')
    fileWriter.close()
    with open(OUTPUT_TARGET_AUTHORS, 'w') as fileWriter:
        for author, confNum in targets.items():
            fileWriter.write(author + '\t' + str(confNum) + '\n')
    fileWriter.close()

if __name__ == '__main__':
    # extracStarsAndTargets()
    baconNum = BaconNumber()
    baconNum.getShortestPathLength()

