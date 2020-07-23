# -*- coding: utf-8 -*-

import re
import json

from typing import Any
from aiohttp import web
from bson.objectid import ObjectId
from bson.errors import InvalidId

from rs21_test.lib.misc import json_dumps


class TwitterByIdHandler(web.View):
    """Twitter handler"""

    def _get_object_id(self) -> ObjectId:
        """
        Convert string into Mongo ObjectId
        :return: ObjectId or rise web.HTTPUnprocessableEntity
        """
        twit_id = self.request.match_info.get('id')
        try:
            obj_id = ObjectId(twit_id)
        except InvalidId:
            resp = {
                "code": 1,
                "type": "error",
                "message": "ID must be a 12-byte input or a 24-character hex string"
            }
            raise web.HTTPUnprocessableEntity(text=json.dumps(resp), content_type='application/json')
        return obj_id

    def _raise_not_found(self) -> Any:
        resp = {
            "code": 1,
            "type": "error",
            "message": "Tweet not found"
        }
        raise web.HTTPNotFound(text=json.dumps(resp), content_type='application/json')

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get tweet by id'
        tags:
          - Twitter
        parameters:
          - name: id
            description: "Tweet ID"
            in: path
            required: true
            schema:
              type: string
        responses:
          '200':
            description: 'Return list of tweets'
          '404':
            description: 'Not found'
          '422':
            description: 'Wrong parameter'
        """

        result = await self.request.app._db.twitter.find_one({"_id": self._get_object_id()})
        if not result:
            return self._raise_not_found()

        return web.json_response(result, dumps=json_dumps)

    async def delete(self) -> web.Response:
        """
        ---
        summary: 'Delete tweet by id'
        tags:
          - Twitter
        parameters:
          - name: id
            description: "Tweet ID"
            in: path
            required: true
            schema:
              type: string
        responses:
          '204':
            description: 'Deleted'
          '404':
            description: 'Did not find Object with this ID'
          '422':
            description: 'Wrong parameter'
        """
        result = await self.request.app._db.twitter.delete_one({"_id": self._get_object_id()})
        if result.deleted_count == 0:
            return self._raise_not_found()

        resp = {
            "code": 0,
            "type": "success",
            "message": "Tweet was deleted"
        }
        return web.json_response(resp, dumps=json_dumps)

    async def patch(self) -> web.Response:
        """
        ---
        summary: 'Patch tweet by id'
        tags:
          - Twitter
        parameters:
          - name: id
            description: "Tweet ID"
            in: path
            required: true
            schema:
              type: string
          - name: username
            description: "Username"
            in: query
            required: false
            schema:
              type: string
          - name: tweet
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
          - name: sentiment
            description: >
              Tweet sentiment:
                * `1` - positive
                * `0` - neutral
                * `-1` - negative
            in: query
            required: false
            schema:
              type: string
        responses:
          '200':
            description: 'Patched'
          '404':
            description: 'Did not find Object with this ID'
          '422':
            description: 'Wrong parameter'
        """

        username = self.request.rel_url.query.get('username')
        tweet = self.request.rel_url.query.get('tweet')
        lat = self.request.rel_url.query.get('lat')
        lon = self.request.rel_url.query.get('lon')
        sentiment = self.request.rel_url.query.get('sentiment')

        new_values = dict()
        if username:
            new_values['username'] = username
        if tweet:
            new_values['tweet'] = tweet
        if lat:
            new_values['lat'] = lat
        if lon:
            new_values['lon'] = lon
        if sentiment:
            new_values['sentiment'] = sentiment

        result = await self.request.app._db.twitter.update_one({"_id": self._get_object_id()}, {"$set": new_values})
        if result.matched_count == 0:
            # Frankly, we just check that object was found, in this case our PATCH reuqest is idempotent. If Mongo sees
            # that values are the same in BD than Mongo won't update these values. If you want to check that values were
            # updated use modified_count method
            return self._raise_not_found()

        resp = {
            "code": 0,
            "type": "success",
            "message": "Tweet was patched"
        }
        return web.json_response(resp, dumps=json_dumps)


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
          - name: sentiment
            description: >
              Tweet sentiment:
                * `1` - positive
                * `0` - neutral
                * `-1` - negative
            in: query
            required: false
            schema:
              type: string
        responses:
          '200':
            description: 'Return list of tweets'
        """

        username = self.request.rel_url.query.get('username')
        query = self.request.rel_url.query.get('query')
        lat = self.request.rel_url.query.get('lat')
        lon = self.request.rel_url.query.get('lon')
        sentiment = self.request.rel_url.query.get('sentiment')
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

        # filter by sentiment
        if sentiment:
            filter_query.update({'sentiment': int(sentiment)})

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

        result = await self.request.app._db.twitter.find(filter_query).to_list(length=None)
        return web.json_response(result, dumps=json_dumps)
