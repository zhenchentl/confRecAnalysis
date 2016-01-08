#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-01-07 20:43:40
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:
#
import sys
from Redis.redisHelper import RedisHelper
from utils.params import *
import random
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class CollaborationFeature(object):
    """docstring for CollaborationFeature"""
    def __init__(self):
        self.mRedis = RedisHelper()

    def isConfLeadCollab(self, author, conf):
        authorConfTime = self.mRedis.getAuConfTimes(author, conf)
        confAuthors = self.mRedis.getConfAuthors(conf)
        coAuthors = self.mRedis.getAuCoauthors(author)
        sameConfCoaus = set(confAuthors) & set(coAuthors)
        if len(sameConfCoaus) != 0:
            for coau in sameConfCoaus:
                coauConfTime = self.mRedis.getAuConfTimes(coau, conf)
                if authorConfTime == coauConfTime:
                    coauTime = self.mRedis.getAuCoauTimes(author, coau)
                    if min(coauTime) > min(authorConfTime):
                        coauTime = []
                        coauConfTime = []
                        authorConfTime = []
                        confAuthors = []
                        coAuthors = []
                        sameConfCoaus = []
                        return True
        coauTime = []
        coauConfTime = []
        authorConfTime = []
        confAuthors = []
        coAuthors = []
        sameConfCoaus = []
        return False

    def getConfLeadCollabProb(self):
        confCountCLCPDictList = dict()
        authors = self.mRedis.getAllAuthors()
        authorDict = dict()
        index = 0
        while index < 200000:
            author = random.choice(authors)
            if authorDict.has_key(author):
                continue
            authorDict[author] = True
            authorConfs = self.mRedis.getAuConfs(author)
            ConfCnt = len(authorConfs)
            if ConfCnt < 2:
                continue
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            CLCPNum = 0
            for conf in authorConfs:
                if self.isConfLeadCollab(author, conf):
                    CLCPNum += 1
            tmp = confCountCLCPDictList.setdefault(ConfCnt, [])
            tmp.append(CLCPNum * 1.0 / ConfCnt)
            authorConfs = []
        authors = []
        with open(OUTPUT_COLLAB_CONF_LEAD_COLLAB_PROB, 'w') as fileWriter:
            for k, v in confCountCLCPDictList.items():
                if len(v) == 0:
                    avg = 0
                else:
                    avg = sum(v) * 1.0 / len(v)
                fileWriter.write(str(k) + '\t' + str(avg))
        fileWriter.close()
        confCountCLCPDictList = {}

if __name__ == '__main__':
    cf = CollaborationFeature()
    cf.getConfLeadCollabProb()
