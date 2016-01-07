#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-01-06 20:34:22
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import sys
from Redis.redisHelper import RedisHelper
from utils.params import *
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def statisticOfList(mList):
    avg = sum(mList) * 1.0 / len(mList)
    max_num = max(mList)
    min_num = min(mList)
    return avg, max_num, min_num

def getLevelWithConfNum():
    mRedis = RedisHelper()
    authors = mRedis.getAllAuthors()
    confPaperNumDict = dict()
    confCoauNumDict = dict()
    index = 0
    for author in authors:
        index += 1
        if index % 1000 == 0:
            logging.info(index)
        confNum = len(mRedis.getAuConfs(author))
        paperNum = len(mRedis.getAuPapers(author))
        coauNum = len(mRedis.getAuCoauthors(author))
        paperNumList = confPaperNumDict.setdefault(confNum, [])
        paperNumList.append(paperNum)
        coauNumList = confCoauNumDict.setdefault(confNum, [])
        coauNumList.append(coauNum)
    index = 0
    with open(OUTPUT_ACADEMIC_LEVEL_PAPER_COAU_NUM,'w') as fileWriter:
        for confNum in confPaperNumDict.keys():
            index += 1
            if index % 1000 == 0:
                logging.info(index)
            paperNumList = confPaperNumDict[confNum]
            avg_P, max_num_P, min_num_P = statisticOfList(paperNumList)
            coauNumList = confCoauNumDict[confNum]
            avg_C, max_num_C, min_num_C = statisticOfList(coauNumList)
            fileWriter.write(str(confNum) + '\t'
                + str(avg_P) + '\t' + str(max_num_P) + '\t' + str(min_num_P) + '\t'
                + str(avg_C) + '\t' + str(max_num_C) + '\t' + str(min_num_C) + '\n')
    fileWriter.close()

if __name__ == '__main__':
    getLevelWithConfNum()


