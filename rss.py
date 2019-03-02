#!/usr/bin/env python3
#-*- coding: UTF 8 -*-
import sys
import json
import time
import base64
import urllib
import logging
import binascii
import telegram
import traceback
import feedparser
import configparser
from datetime import datetime
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, ForeignKey, update, and_


Base = declarative_base()


class Source(object):
	""" 用于解析RSS提要的类。
            从一般信息中仅选择我们感兴趣的字段：标题，链接，发布日期。
	"""
	def __init__(self, config_links):
		self.links = [config_links[i] for i in config_links]
		self.news = []
		self.refresh()

	def refresh(self):
		self.news = []
		for i in self.links:
			data = feedparser.parse(i)
			self.news += [News(binascii.b2a_base64(i['title'].encode()).decode(),\
				binascii.b2a_base64(i['link'].encode()).decode(),\
				int(time.mktime(i['published_parsed']))) for i in data['entries']] 

	def __repr__(self):
		return "<RSS ('%s','%s')>" % (self.link, len(self.news))

class Bitly:
	def __init__(self,access_token):
		self.access_token = access_token

	def short_link(self, long_link):
		url = 'https://api-ssl.bitly.com/v3/shorten?access_token=%s&longUrl=%s&format=json'\
		 % (self.access_token, long_link)
		try:
			return json.loads(urllib.request.urlopen(url).read().decode('utf8'))['data']['url']
		except:
			return long_link

class News(Base):	
	"""
	描述新闻项的类。 此外，与数据库的交互。下表中字段的描述。
	"""
	__tablename__ = 'news'
	id = Column(Integer, primary_key=True) # 订单号新闻
	text = Column(String) # 要在消息中发送的文本（标题）
	link  = Column(String) # 链接到网站上的文章。 还发送了一条消息
	date = Column(Integer)
	# 网站上的新闻发布日期。 这纯粹是信息性的。UNIX_TIME。
	publish = Column(Integer)
	# 计划发布日期。 该消息将在此日期之前发送。UNIX_TIME。
	chat_id = Column(Integer) 
	# 信息栏。 在此版本中，功能负载不携带。
	message_id = Column(Integer) 
	# 信息栏。 在此版本中，功能负载不携带。

	def __init__(self, text, link, date, publish=0,chat_id=0,message_id=0):
		self.link = link
		self.text  = text
		self.date = date
		self.publish = publish
		self.chat_id = chat_id
		self.message_id = message_id

	def _keys(self):
		return (self.text, self.link)

	def __eq__(self, other):
		return self._keys() == other._keys()

	def __hash__(self):
		return hash(self._keys())

	def __repr__(self):
		return "<News ('%s','%s', %s)>" % (base64.b64decode(self.text).decode(),\
			base64.b64decode(self.link).decode(),\
			datetime.fromtimestamp(self.publish))
			＃对于视觉感知数据被解码

class Database:
	"""
	用于处理SQLAlchemy会话的类。它还包括控件类中调用的最小方法集。方法的名称说。
	"""
	def __init__(self, obj):
		engine = create_engine(obj, echo=False)
		Session = sessionmaker(bind=engine)
		self.session = Session()
	
	def add_news(self, news):
		self.session.add(news)
		self.session.commit()

	def get_post_without_message_id(self):
		return self.session.query(News).filter(and_(News.message_id == 0,\
					News.publish<=int(time.mktime(time.localtime())))).all()

	def update(self, link, chat, msg_id):
		self.session.query(News).filter_by(link = link).update({"chat_id":chat, "message_id":msg_id})
		self.session.commit()

	def find_link(self,link):
		if self.session.query(News).filter_by(link = link).first(): return True
		else: return False 
	
class ExportBot:
	def __init__(self):
		config = configparser.ConfigParser()
		config.read('./config')
		log_file = config['Export_params']['log_file']
		self.pub_pause = int(config['Export_params']['pub_pause'])
		self.delay_between_messages = int(config['Export_params']['delay_between_messages'])
		logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s] %(message)s',level = logging.INFO, filename = u'%s'%log_file)
		self.db = Database(config['Database']['Path'])
		self.src = Source(config['RSS'])
		self.chat_id = config['Telegram']['chat']
		bot_access_token = config['Telegram']['access_token']
		self.bot = telegram.Bot(token=bot_access_token)
		self.bit_ly = Bitly(config['Bitly']['access_token'])
	
	def detect(self):
		＃我们从rss-channel获得最后30个帖子
		self.src.refresh()
		news = self.src.news		
		news.reverse()
		＃检查数据库中是否存在新闻链接，如果没有，则添加到数据库中
                ＃推迟出版
		for i in news:
			if not self.db.find_link(i.link):
				now = int(time.mktime(time.localtime()))
				i.publish = now + self.pub_pause
				logging.info( u'Detect news: %s' % i)
				self.db.add_news(i)

	def public_posts(self):
		＃我们从rss频道获得30个最近的条目，从数据库获得新闻，其中message_id = 0
		posts_from_db = self.db.get_post_without_message_id()
		self.src.refresh()
		line = [i for i in self.src.news]
		＃选择这些列表的交叉线
		for_publishing = list(set(line) & set(posts_from_db))
		for_publishing = sorted(for_publishing, key=lambda news: news.date)
		＃发布每条消息
		for post in for_publishing:
			text = '%s %s' % (base64.b64decode(post.text).decode('utf8'),\
							  self.bit_ly.short_link(base64.b64decode(post.link).decode('utf-8')))
			a = self.bot.sendMessage(chat_id=self.chat_id, text=text, parse_mode=telegram.ParseMode.HTML)
			message_id = a.message_id
			chat_id = a['chat']['id']
			self.db.update(post.link, chat_id, message_id)
			logging.info( u'Public: %s;%s;' % (post, message_id))
			time.sleep(self.delay_between_messages)
