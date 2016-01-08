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

    def isCoauLeadByConf(self, author, coau):
        auCoauTimes = self.mRedis.getAuCoauTimes(author, coau)
        authorConfs = self.mRedis.getAuConfs(author)
        coauConfs = self.mRedis.getAuConfs(coau)
        sameConfs = set(authorConfs) & set(coauConfs)
        if len(sameConfs) > 1:
            confTimes = list()
            for conf in sameConfs:
                confTimes.extend(self.mRedis.getAuConfTimes(author, conf))
            if min(auCoauTimes) > min(confTimes):
                auCoauTimes = []
                authorConfs = []
                coauConfs = []
                sameConfs = []
                confTimes = []
                return True
        auCoauTimes = []
        authorConfs = []
        coauConfs = []
        sameConfs = []
        confTimes = []
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
                fileWriter.write(str(k) + '\t' + str(avg) + '\n')
        fileWriter.close()
        confCountCLCPDictList = {}

    def getCoauLeadByConf(self):
        ConfCountCoauDictList = dict()
        authors = self.mRedis.getAllAuthors()
        authorDict = dict()
        index = 0
        while index < 200000:
            author = random.choice(authors)
            if authorDict.has_key(author):
                continue
            authorDict[author] = True
            auCoauthors = self.mRedis.getAuCoauthors(author)
            authorConfs = self.mRedis.getAuConfs(author)
            confCnt = len(authorConfs)
            if confCnt < 2:
                continue
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            CLCsNum = 0
            for coau in auCoauthors:
                if self.isCoauLeadByConf(author, coau):
                    CLCsNum += 1
            tmp = ConfCountCoauDictList.setdefault(confCnt, [])
            tmp.append(CLCsNum)
            authorConfs = []
            auCoauthors = []
        authors = []
        with open(OUTPUT_COLLAB_COAU_NUM_LEAD_BY_CONF, 'w') as fileWriter:
            for k, v in ConfCountCoauDictList.items():
                if len(v) == 0:
                    avg = 0
                else:
                    avg = sum(v) * 1.0 / len(v)
                fileWriter.write(str(k) + '\t' + str(avg) + '\n')
        fileWriter.close()
        ConfCountCoauDictList = {}

    def getConfLeadPotentialCoaus(self):
        ConfCountPotentialCoausDict = dict()
        confAuthorDict = dict()
        confs = self.mRedis.getAllConfs()
        for conf in confs:
            confAuthorDict[conf] = self.mRedis.getConfAuthors(conf)
        authors = self.mRedis.getAllAuthors()
        authorDict = dict()
        index = 0
        while index < 200000:
            author = random.choice(authors)
            if authorDict.has_key(author):
                continue
            authorDict[author] = True
            if index % 1000 == 0:
                logging.info(index)
            confs = self.mRedis.getAuConfs(author)
            potentialCoaus = list()
            for conf in confs:
                potentialCoaus.extend(confAuthorDict[conf])
            coAuthors = self.mRedis.getAuCoauthors(author)
            PotenCoauNum = len(set(potentialCoaus) - set(coAuthors))
            tmp = ConfCountPotentialCoausDict.setdefault(PotenCoauNum, [])
            tmp.append(PotenCoauNum)
            confs = []
            potentialCoaus = []
            coAuthors = []
        authors = []
        confAuthorDict = {}
        with open(OUTPUT_COLLAB_CONF_LEAD_POTENRIAL_COAU, 'w') as fileWriter:
            for k, v in ConfCountPotentialCoausDict.items():
                if len(v) == 0:
                    avg = 0
                else:
                    avg = sum(v) * 1.0 / len(v)
                fileWriter.write(str(k) + '\t' + str(avg) + '\n')
        fileWriter.close()
        ConfCountPotentialCoausDict = {}

if __name__ == '__main__':
    cf = CollaborationFeature()
    # cf.getConfLeadCollabProb()
    # cf.getCoauLeadByConf()
    cf.getConfLeadPotentialCoaus()
