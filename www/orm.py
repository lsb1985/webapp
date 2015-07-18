# -*- coding:utf-8 -*-

__author__='caibird'

'''
orm for mysql
'''

import asyncio,logging
import aiomysql


#---------------------------------
#function:log for sql
#input:sql
#output:log info
#history
#	20150718	init
#---------------------------------
def log(sql,args=()):
	logging.info('SQL:%s'%sql)


#---------------------------------
#function:create sql connection pool
#input:loop
#output:create sql connection
#history
#	20150718	init
#---------------------------------
@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info('create database connection pool...')
	global __pool
	__pool=yield from aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
		)


#---------------------------------
#function:select from mysql
#input:sql,args,size
#output:retrun select rows
#history
#	20150718	init
#---------------------------------
@asyncio.coroutine
def select(sql,args,size=None):
	log(sql,args)
	global __pool
	with (yield from __pool) as conn:
		cur=yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.execute(sql.replace('?','%s'),args or ())
		if size:
			rs=yield from cur.fetchmany(size)
		else:
			rs=yield from cur.fetchall()
		yield from cur.close()
		logging.info('rows return:%s' %len(rs))	
		return rs
		

#---------------------------------
#function:execute:insert,update,delete
#input:sql,args
#output:retrun affected rowcount
#history
#	20150718	init
#---------------------------------
@asyncio.coroutine
def execute(sql,args):
	log(sql)
	global __pool
	with (yield from __pool) as conn:
		try:
			cur=yield from conn.cursor()
			yield from cur.execute(sql.replace('?','%s'),args)
			affected=cur.rowcount
			yield from cur.close()
		except BaseException, e:
			raise
		return affected

