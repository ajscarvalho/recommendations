import argparse
import asyncio
import logging
import sys # argv

#import jinja2

#import aiohttp_jinja2
from aiohttp import web # webserver

#configuration
from lib.config.config import Config
from trafaret_config import commandline

#webserver aux - TODO session
from .middlewares   import setup_middlewares
from .routes        import setup_routes

from db import setup_kvs, kvs_connect, kvs_disconnect


def init(loop, argv):
    ap = argparse.ArgumentParser()
    commandline.standard_argparse_options(ap, default_config='./config/webserver.yaml')
    #
    # define your command-line arguments here
    #
    options = ap.parse_args(argv)
    config = commandline.config_from_options(options, CONFIG_PARSER)

    # setup application and extensions
    app = web.Application(loop=loop)

    # load config from yaml file in current dir
    app['config'] = config['webserver']

    # set up kvs db (redis)
    setup_kvs(app, loop, config['db']['redis'])

    # setup Jinja2 template renderer
    #aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader('featrecommender', 'templates'))

    # create connection to the database
    app.on_startup.append(kvs_connect)

    # shutdown db connection on exit
    app.on_cleanup.append(kvs_disconnect)

    # setup views and routes
    setup_routes(app)
    setup_middlewares(app)

    return app




def main(argv):
    # init logging - replace?
    logging.basicConfig(level=logging.DEBUG)

    loop = asyncio.get_event_loop()

    app = init(loop, argv)
    web.run_app(app,
                host=app['config']['host'],
                port=app['config']['port'])


if __name__ == '__main__':
    main(sys.argv[1:])

