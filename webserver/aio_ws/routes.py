import pathlib

#from .views import index, poll, results, vote
from .views.homepage import homepage

from .api.actions.catalog            import catalog_import
from .api.actions.learning_actions   import learn
from .api.requests.recommend         import recommend


PROJECT_ROOT = pathlib.Path(__file__).parent


def setup_routes(app):
    app.router.add_get ('/',              homepage)
    app.router.add_post('/api/catalog',   catalog_import, name='catalog')
    app.router.add_post('/api/learn',     learn,          name='learn')
    app.router.add_get ('/api/recommend', recommend,      name='recommend')
#    app.router.add_post('/poll/{question_id}/vote', vote, name='vote')

#    app.router.add_static('/static/',
#                          path=str(PROJECT_ROOT / 'static'),
#                          name='static')