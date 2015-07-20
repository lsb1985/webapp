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
#function:sql:insert,update,delete
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


class Field(object):
	def __init__(self,name,column_type,primary_key,default):
		self.name=name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default=default

	def __str__(self):
		return '<%s,%s:%s>' %(self.__calss__.__name__,self.column_type,self.name)

class StringField(Field):
	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)


class BooleanField(Field):
	def __init__(self,name=None,default=False):
		super().__init__(name,'boolean',False,default)

class IntegerField(Field):
	def __init__(self,name=None,primary_key=False,default=0):
		super().__init__(name,'bigint',primary_key,default)	

class FloatField(Field):
	def __init__(self,name=None,primary_key=False,default=0.0):
		super().__init__(name,'real',primary_key,default)

class TextField(Field):
	super().__init__(name,'text',False,default)

#任何继承自Model的类（比如User），会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性如__table__、__mappings__中
class ModelMetaclass(type):
	def __new__(cls,name,bases,attrs):
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)
		tableName=attrs.get('__table__',None) or name
		logging.info("found model:%s(table:%s)" %(name,tableName))

		mppings=ditc()
		field=[]
		primaryKey=None
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info(" found mapping:%s==%s"%(k,v))
				mapping[k]=v
			if v.primary_key:
				if primaryKey:
					raise StandarError('Duplicate primary key for field:%s'%k)
				primaryKey=k
			else:
				fields.append(k)
		if not primaryKey:
			raise StandarError('Primary key not found')
		for k in mapping.keys():
			attrs.pop(k)
		escaped_fields=list(map(lambda f:'`%s`'%f,fields))
		attrs['__mappings__']=mappings
		attrs['__table__']=tableName
		attrs['__primary_key__']=primary_key
		
		#主键外的属性名
		attrs['__fields__']=fields 
		
		#构造默认的select，insert，update，delete
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


#---------------------------------
#function:base calss Model
#input:
#output:
#history
#	20150718	init
#---------------------------------
class Model(dict,metaclass=ModelMetaclass):
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'"%key)
	
	def __setattr__(sefl,key,value):
		self[key]=value

	def getValue(self,key):
		return getattr(self,key,None)
		
	def getValueDefault(self,key):
		value=getattr(self,key,None)
		field=self.__mapping__[key]
		if field.default is not None:
			value=field.default() if callable(field.default) else field.default
			logging.debug('using default value for %s:%s' %(key,str(value)))
			setattr(self,key,value)
		return value


	@classmethod
	@asyncio.coroutine
	def find(cls,pk):
		'find object by primary key.'
		rs=yield from select('%s where `%s`=?' %(cls.__select__,cls.__primary_key__),[pk],1)
		if len(rs)==0:
			return None
		return cls(**rs[0])

	@classmethod
	@asyncio.coroutine
	def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = yield from select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    @asyncio.coroutine
    def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)
