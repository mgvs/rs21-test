#!/apps/miniconda3/envs/rs21/bin/python3
# -*- coding: utf-8 -*-

"""RS21 API """

import logging
import argparse
import yaml

from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, SwaggerUiSettings

from rs21_test.lib.db import DatabaseConfig
from rs21_test.app.handlers.facebook import FacebookHandler
from rs21_test.app.handlers.twitter import TwitterHandler
from rs21_test.app.handlers.bernallio import BernallioHandler


def main():
    """Run RS21 API"""

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', help='config file', required=True)
    args = parser.parse_args()

    app = web.Application()

    with open(args.config, 'r') as fd:
        app.cfg = yaml.safe_load(fd)

    app._db = DatabaseConfig.asyncmongo(
        app.cfg['MONGO_DB']['HOST'],
        app.cfg['MONGO_DB']['PORT'],
        app.cfg['MONGO_DB']['DB_NAME']
    )

    handlers = [
        web.get('/api/v1/facebook', FacebookHandler),
        web.get('/api/v1/twitter', TwitterHandler),
        web.get('/api/v1/bernallio', BernallioHandler)
    ]

    swagger = SwaggerDocs(
        app,
        swagger_ui_settings=SwaggerUiSettings(path='/api/v1/docs'),
        title='RS21 API',
        version='0.1'
    )

    swagger.add_routes(handlers)
    web.run_app(app, host=app.cfg['APP']['HOST'], port=int(app.cfg['APP']['PORT']))


if __name__ == '__main__':
    main()
