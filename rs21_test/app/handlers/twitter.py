# -*- coding: utf-8 -*-

import csv
import datetime
import re

from aiohttp import web

from rs21_test.lib.misc import json_dumps


class TwitterHandler(web.View):
    """Twitter handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get tweets list'
        tags:
          - Twitter
        parameters:
          - name: username
            description: "Username"
            in: query
            required: false
            schema:
              type: string
          - name: query
            description: "Tweet content"
            in: query
            required: false
            schema:
              type: string
          - name: lat
            description: "Tweet geo location, latitude"
            in: query
            required: false
            example: 35.08063
            schema:
              type: number
          - name: lon
            description: "Tweet geo location, longitude"
            in: query
            required: false
            example: -106.37636
            schema:
              type: number
          - name: dist
            description: "Distance in meters from geo position"
            in: query
            required: false
            example: 100
            schema:
              type: number
        responses:
          '200':
            description: 'Return list of tweets'
        """

        username = self.request.rel_url.query.get('username', None)
        query = self.request.rel_url.query.get('query', None)
        lat = self.request.rel_url.query.get('lat', None)
        lon = self.request.rel_url.query.get('lon', None)
        dist = int(self.request.rel_url.query.get('dist', 100))

        filter_query = {}

        # filter by user name
        if username:
            q_str = re.compile(r'^{}$'.format(username), re.I)
            filter_query.update({'username': q_str})

        # filter by tweet content
        if query:
            q_str = re.compile(r'^.*?{}.*?$'.format(' '.join(query.split()).replace(' ', '\s+')), re.I)
            filter_query.update({'tweet': q_str})

        # geo filter
        if all([lon, lat]):
            # query = {'geo': {"$within": {"$polygon": polygon_geo}}}
            filter_query.update({
                'location': {
                    '$near': {
                        '$geometry': {
                            'type': "Point",
                            'coordinates': [float(lon), float(lat)]},
                        '$maxDistance': dist
                    }
                }
            })  # 100 meters

        result = await self.request.app._db.twitter.find(filter_query, {'_id': 0}).to_list(length=None)
        return web.json_response(result, dumps=json_dumps)
