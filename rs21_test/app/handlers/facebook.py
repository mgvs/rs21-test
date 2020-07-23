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
            description: "Name of place (or multiple with comma separated)"
            in: query
            required: false
            schema:
              type: string
          - name: type
            description: "Place type (or multiple with comma separated)"
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

        filter_query = []

        if place:
            place_queries = []
            for place_query in place.split(','):
                q_str = re.compile(r'^.*?{}.*?$'.format(place_query.rstrip(' ').lstrip(' ').replace(r' ', r'\s+')), re.IGNORECASE)
                place_queries.append({'place': q_str})
            filter_query.append({
                '$or': place_queries
            })

        if place_type:
            place_queries = []
            for place_type_query in place_type.split(','):
                q_str = re.compile(r'^.*?{}.*?$'.format(place_type_query.rstrip(' ').lstrip(' ').replace(r' ', r'\s+')), re.IGNORECASE)
                place_queries.append({'type': q_str})
            filter_query.append({
                '$or': place_queries
            })

        if all([lon, lat]):
            filter_query.append({
                'location': {
                    '$near': {
                        '$geometry': {
                           'type': "Point",
                            'coordinates': [float(lon), float(lat)]},
                        '$maxDistance': dist
                    }
                }
            })  # 100 meters

        query_filter = {}
        if filter_query:
            query_filter = {"$and": filter_query}

        result = await self.request.app._db.facebook.find(query_filter, {'_id': 0}).to_list(length=None)
        return web.json_response(result, dumps=json_dumps)


class FacebookTypePlacesHandler(web.View):
    """Facebook All Places handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get list of all types of Facebook places'
        tags:
          - Facebook
        responses:
          '200':
            description: 'Return list of all types of Facebook places'
        """

        all_types = list()
        pipline = [{"$sort": {"type": 1}}, {"$group": {"_id": "$type"}}]
        async for doc in self.request.app._db.facebook.aggregate(pipline):
            all_types.append(doc.get('_id'))
        return web.json_response({"all_types": all_types}, dumps=json_dumps)

