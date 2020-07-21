# -*- coding: utf-8 -*-

import csv
import datetime
import re

from aiohttp import web

from rs21_test.lib.misc import json_dumps


class BernallioHandler(web.View):
    """Bernallio handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get Bernallio Census data'
        tags:
          - Bernallio
        parameters:
          - name: lat
            description: "Facebook geo location, latitude"
            in: query
            required: false
            example: 35.0480546
            schema:
              type: number
          - name: lon
            description: "Facebook geo location, longitude"
            in: query
            required: false
            example: -106.7204881
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
            description: 'Return Bernallio Census data'
        """

        lat = self.request.rel_url.query.get('lat', None)
        lon = self.request.rel_url.query.get('lon', None)
        dist = int(self.request.rel_url.query.get('dist', 100))

        filter_query = {}

        if all([lon, lat]):
            filter_query.update({
                'properties.location': {
                    '$near': {
                        '$geometry': {
                           'type': "Point",
                            'coordinates': [float(lon), float(lat)]},
                        '$maxDistance': dist
                    }
                }
            })  # 100 meters

        result = await self.request.app._db.bernallio.find(filter_query, {'_id': 0}).to_list(length=None)
        return web.json_response(result, dumps=json_dumps)
