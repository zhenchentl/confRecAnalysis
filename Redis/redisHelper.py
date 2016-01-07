#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-01-04 18:31:35
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import sys
import redis
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

IP = '127.0.0.1'
PORT = 6379

'''key-->set: 学者所有合作者。author-->(coau1, coau2, coau3...)'''
DB_AU_COAU_SET = 0
'''key-->set：学者参加的会议。author-->(conf1, conf2,conf3...)'''
DB_AU_CONF_SET = 1
'''key-->set：学者发表的所有论文。author-->(paper1, paper2, paper3...)'''
DB_AU_PAPER_SET = 2
'''key-->set：会议中所有学者。conf-->(author1, author2, author3...)'''
DB_CONF_AU_SET = 3
'''key-->set：会议收录的所有论文。conf-->(paper1, paper2, paper3...)'''
DB_CONF_PAPER_SET = 4
'''key-->set：两个人合作的所有合作时间。author:author-->(time1, time2, time3...)'''
DB_AU_COAU_TIME_SET = 5
'''key-->set：学者参会时间。author:conf-->(time1, time2, time3...)'''
DB_AU_CONF_TIME_SET = 6
'''key-->value：论文发表时间'''
DB_PAPER_TIME = 7

class RedisHelper(object):
    """访问Redis数据库，基本的数据操作。"""
    def __init__(self):
        try:
            self.AuCoauSet = redis.StrictRedis(IP, PORT, db = DB_AU_COAU_SET)
            self.AuConfSet = redis.StrictRedis(IP, PORT, db = DB_AU_CONF_SET)
            self.AuPaperSet = redis.StrictRedis(IP, PORT, db = DB_AU_PAPER_SET)
            self.ConfAuSet = redis.StrictRedis(IP, PORT, db = DB_CONF_AU_SET)
            self.ConfPaperSet = redis.StrictRedis(IP, PORT, db = DB_CONF_PAPER_SET)
            self.AuCoauTimeSet = redis.StrictRedis(IP, PORT, db = DB_AU_COAU_TIME_SET)
            self.AuConfTimeSet = redis.StrictRedis(IP, PORT, db = DB_AU_CONF_TIME_SET)
            self.PaperTime = redis.StrictRedis(IP, PORT, db = DB_PAPER_TIME)
        except:
            logging.error("can not open Redis database!")

    def addPaperItem(self, authors, paperId, conf, year):
        """ 增加一条论文的信息到redis数据库中。

        Arguments:
            authors {String} -- 学者姓名
            paperId {Integer} -- 论文ID
            conf {String} -- 会议/期刊名称
            year {String} -- 发表年份
        """
        self.addConfPaper(conf, paperId)
        for au in authors:
            self.addAuConf(au, conf, year)
            self.addAuPaper(au, paperId)
            self.addConfAuthor(conf, au)
            self.addPaperTime(paperId, year)
            for coau in authors:
                if au != coau:
                    self.addAuCoauthor(au, coau, year)

    def addAuCoauthor(self, author, coauthor, time):
        self.AuCoauSet.sadd(author, coauthor)
        self.AuCoauTimeSet.sadd(author + ':' + coauthor, time)

    def addAuConf(self, author, conf, time):
        self.AuConfSet.sadd(author, conf)
        self.AuConfTimeSet.sadd(author + ':' + conf, time)

    def addAuPaper(self, author, paper):
        self.AuPaperSet.sadd(author, paper)

    def addConfPaper(self, conf, paperId):
        self.ConfPaperSet.sadd(conf, paperId)

    def addConfAuthor(self, conf, author):
        self.ConfAuSet.sadd(conf, author)

    def addPaperTime(self, paperId, time):
        self.PaperTime.set(paperId, time)

    def getAllAuthors(self):
        return self.AuConfSet.keys()

    def getAllConfs(self):
        return self.ConfPaperSet.keys()

    def getAllPapers(self):
        return self.PaperTime.keys()

    def getAuCoauthors(self, author):
        return self.AuCoauSet.smembers(author)

    def getAuConfs(self, author):
        return self.AuConfSet.smembers(author)

    def getAuPapers(self, author):
        return self.AuPaperSet.smembers(author)

    def getConfAuthors(self, conf):
        return self.ConfAuSet.smembers(conf)

    def getConfPapers(self, conf):
        return self.ConfPaperSet.smembers(conf)

    def getAuCoauTimes(self, author, coauthor):
        return self.AuCoauTimeSet.smembers(author + ':' + coauthor)

    def getAuConfTimes(self, author, conf):
        return self.AuConfTimeSet.smembers(author + ':' + conf)

    def getPaperTime(self, paperId):
        return self.PaperTime.get(paperId)

if __name__ == '__main__':
    pass
