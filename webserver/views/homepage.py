#import aiohttp_jinja2
from aiohttp import web

async def homepage(request): return web.Response(status=200, body="Under DEV")
#@aiohttp_jinja2.template('index.html')
#async def homepage(request):
#    async with request.app['db'].acquire() as conn:
#        cursor = await conn.execute(db.question.select())
#        records = await cursor.fetchall()
#        questions = [dict(q) for q in records]
#        return {'questions': questions}