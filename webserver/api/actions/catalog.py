from aiohttp import web

async def catalog_import(request): raise web.HTTPNotImplemented(text="TBD")

#async def homepage(request):
#    async with request.app['db'].acquire() as conn:
#        cursor = await conn.execute(db.question.select())
#        records = await cursor.fetchall()
#        questions = [dict(q) for q in records]
#        return {'questions': questions}