# -*- coding: utf-8 -*-

import re

from aiohttp import web

from rs21_test.lib.misc import json_dumps


class FacebookHandler(web.View):
    """Facebook handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get Facebook places'
        tags:
          - Facebook
        parameters:
          - name: query
            description: "Name of place"
            in: query
            required: false
            schema:
              type: string
          - name: type
            description: "Place type"
            in: query
            required: false
            schema:
              type: string
          - name: lat
            description: "Facebook geo location, latitude"
            in: query
            required: false
            example: 35.05917399
            schema:
              type: number
          - name: lon
            description: "Facebook geo location, longitude"
            in: query
            required: false
            example: -106.5821513
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
            description: 'Return list of Facebook places'
        """

        place = self.request.rel_url.query.get('query', None)
        place_type = self.request.rel_url.query.get('type', None)
        lat = self.request.rel_url.query.get('lat', None)
        lon = self.request.rel_url.query.get('lon', None)
        dist = int(self.request.rel_url.query.get('dist', 100))

        filter_query = {}

        if place:
            q_str = re.compile(r'^.*?{}.*?$'.format(place.replace(' ', '\s+')), re.IGNORECASE)
            filter_query.update({'place': q_str})

        if place_type:
            q_str = re.compile(r'^.*?{}.*?$'.format(place_type.replace(' ', '\s+')), re.IGNORECASE)
            filter_query.update({'type': q_str})

        if all([lon, lat]):
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

        result = await self.request.app._db.facebook.find(filter_query, {'_id': 0}).to_list(length=None)
        return web.json_response(result, dumps=json_dumps)


class FacebookAllPlacesHandler(web.View):
    """Facebook All Places handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get list of all Facebook places'
        tags:
          - FacebookAllPlaces
        responses:
          '200':
            description: 'Return list of all Facebook places'
        """

        place = self.request.rel_url.query.get('query', None)
        place_type = self.request.rel_url.query.get('type', None)
        lat = self.request.rel_url.query.get('lat', None)
        lon = self.request.rel_url.query.get('lon', None)
        dist = int(self.request.rel_url.query.get('dist', 100))

        filter_query = {}

        if place:
            q_str = re.compile(r'^.*?{}.*?$'.format(place.replace(' ', '\s+')), re.IGNORECASE)
            filter_query.update({'place': q_str})

        if place_type:
            q_str = re.compile(r'^.*?{}.*?$'.format(place_type.replace(' ', '\s+')), re.IGNORECASE)
            filter_query.update({'type': q_str})

        if all([lon, lat]):
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

        result = await self.request.app._db.facebook.find({}, {'_id': 0}).to_list(length=None)
        return web.json_response({"all_places": {i.get('place') for i in result}}, dumps=json_dumps)

