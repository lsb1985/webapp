# -*- coding:utf-8 -*-

__author__='caibird'

'''
async web application
'''

import asyncio,json,os,time,logging
from datetime import datetime
from aiohttp import web

#------ variable ------------
srv_ip='127.0.0.1'
srv_port=9000

#---------------------------------
#function:process url,like:/index
#input:request
#output:response index body
#history
#	20150718	init
#---------------------------------
def index(request):
	return web.Response(body=b'<h1>Awesome</h1>')

#---------------------------------
#function:init server
#input:loop 
#output:
#history
#	20150718	init
#---------------------------------
@asyncio.coroutine
def init(loop):
	app=web.Application(loop=loop)
	app.router.add_route('GET','/',index)
	srv=yield from loop.create_server(app.make_handler(),srv_ip,srv_port)
	logging.info("server started at http://%s:%d" %(srv_ip,srv_port))


loop=asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()