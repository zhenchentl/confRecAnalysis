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
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
