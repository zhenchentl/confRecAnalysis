#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-01-06 10:47:54
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import sys
from Redis.redisHelper import RedisHelper
from utils.params import *
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def getConfPaperNum():
    """统计每个会议收录了多少论文。

    格式：会议名-->收录论文数
    """
    index = 0
    with open(OUTPUT_STATISTIC_CONF_PAPER_NUM, 'w') as fileWriter:
        mRedis = RedisHelper()
        confPaperNum = dict()
        conferences =mRedis.getAllConfs()
        for conf in conferences:
            index += 1
            logging.info(str(index) + '--' + conf)
            paperNum = len(mRedis.getConfPapers(conf))
            tmp = confPaperNum.setdefault(paperNum, 0)
            confPaperNum[paperNum] = tmp + 1
        for paperNum, confNum in confPaperNum.items():
            fileWriter.write(str(paperNum) + '\t' + str(confNum) + '\n')
    fileWriter.close()

def getAuthorPaperNum():
    """统计每个学者发表了多少论文。

    格式：论文数-->学者数
    """
    index = 0
    with open(OUTPUT_STATISTIC_AUTHOR_PAPER_NUM, 'w') as fileWriter:
        mRedis = RedisHelper()
        authors = mRedis.getAllAuthors()
        authorPaperNum = dict()
        for author in authors:
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            paperNum = len(mRedis.getAuPapers(author))
            tmp = authorPaperNum.setdefault(paperNum, 0)
            authorPaperNum[paperNum] = tmp + 1
        for k, v in authorPaperNum.items():
            fileWriter.write(str(k) + '\t' + str(v) + '\n')
    fileWriter.close()

def getAuthorCoauNum():
    """统计每个学者的合作者数。

    格式：合作者数-->学者数
    """
    index = 0
    with open(OUTPUT_STATISTIC_AUTHOR_COAU_NUM, 'w') as fileWriter:
        mRedis = RedisHelper()
        authors = mRedis.getAllAuthors()
        authorCoauNum = dict()
        for author in authors:
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            coauNum = len(mRedis.getAuCoauthors(author))
            tmp = authorCoauNum.setdefault(coauNum, 0)
            authorCoauNum[coauNum] = tmp + 1
        for k, v in authorCoauNum.items():
            fileWriter.write(str(k) + '\t' + str(v) + '\n')
    fileWriter.close()

def getAuthorConfNum():
    """统计每个学者的参会数。

    格式：参会数-->学者数
    """
    index = 0
    with open(OUTPUT_STATISTIC_AUTHOR_CONF_NUM, 'w') as fileWriter:
        mRedis = RedisHelper()
        authors = mRedis.getAllAuthors()
        authorConfNum = dict()
        for author in authors:
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            confNum = len(mRedis.getAuConfs(author))
            tmp = authorConfNum.setdefault(confNum, 0)
            authorConfNum[confNum] = tmp + 1
        for k, v in authorConfNum.items():
            fileWriter.write(str(k) + '\t' + str(v) + '\n')
    fileWriter.close()

if __name__ == '__main__':
    print 'hello word!'
    # getConfPaperNum()
    # getAuthorPaperNum()
    # getAuthorCoauNum()
    # getAuthorConfNum()
    with open(OUTPUT_STATISTIC_CONF_PAPER_NUM) as fileReader:
        confPaperNumDict = dict()
        for line in fileReader:
            paperNum = int(line.split('\t')[0])
            confs = int(line.split('\t')[1])
            tmp = confPaperNumDict.setdefault(paperNum / 100, 0)
            confPaperNumDict[paperNum / 100] = tmp + confs
        for i in range(1000):
            if confPaperNumDict.has_key(i):
                print str(i * 100) + '-' + str((i+1)*100 - 1 ) + '\t' + str(confPaperNumDict[i])
            else:
                print str(i * 100) + '-' + str((i+1)*100 - 1 ) + '\t' + '0'
    fileReader.close()
