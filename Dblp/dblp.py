#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-01-04 20:55:24
# @Author  : Damon Chen (Damon@zhenchen.me)
# @Link    : www.zhenchen.me
# @Version : $Id$
# @Description:

import sys
sys.path.append('..')
from Redis.redisHelper import RedisHelper
from xml.sax import handler, make_parser
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

paperLabels = ('article','inproceedings','proceedings','book', 'incollection','phdthesis', \
    'mastersthesis','www')

class dblpHandler(handler.ContentHandler):
    """docstring for dblpHandler"""
    def __init__(self):
        self.mRedis = RedisHelper()
        self.isPaperTag = False
        self.conf = ''
        self.isTitleTag = False
        self.paperId = -1
        self.isAuthorTag = False
        self.authors = list()
        self.isYearTag = False
        self.year = ''

    def startDocument(self):
        logging.info('Document start...')

    def endDocument(self):
        logging.info('Document End...')

    def startElement(self, name, attrs):
        if name in paperLabels:
            self.conf = attrs.get('key').split('/')[1]
            self.isPaperTag = True
        if self.isPaperTag:
            if name == 'title':
                self.isTitleTag = True
            if name == 'author':
                self.isAuthorTag = True
            if name == 'year':
                self.isYearTag = True

    def endElement(self, name):
        if name in paperLabels:
            if self.isPaperTag:
                self.isPaperTag = False
                self.mRedis.addPaperItem(self.authors, self.paperId, self.conf, self.year)
                if self.paperId % 1000 == 0:
                    logging.info(self.paperId)
                self.conf = ''
                self.authors = []
                self.year = ''

    def characters(self, content):
        if self.isTitleTag:
            self.paperId += 1
            self.isTitleTag = False
        if self.isYearTag:
            self.year = content
            self.isYearTag = False
        if self.isAuthorTag:
            self.authors.append(content)
            self.isYearTag = False

def parserDblpXml():
    handler = mHandler()
    parser = make_parser()
    parser.setContentHandler(handler)
    f = open(DBLP_XML_PATH,'r')
    parser.parse(f)
    f.close()

 if __name__ == '__main__':
     parserDblpXml()
