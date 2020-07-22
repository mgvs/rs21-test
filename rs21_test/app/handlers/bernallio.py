# -*- coding: utf-8 -*-

import csv
import datetime
import re

from aiohttp import web

from rs21_test.lib.misc import json_dumps

MIN_AGE = 0
MAX_AGE = 130


class BernallioHandler(web.View):
    """Bernallio handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get Bernallio Census data'
        tags:
          - Census
        parameters:
          - name: agemin
            description: "Age from"
            in: query
            required: false
            example: 18
            schema:
              type: number
          - name: agemax
            description: "Age before"
            in: query
            required: false
            example: 56
            schema:
              type: number
          - name: gender
            description: "Gender"
            in: query
            schema:
              type: string
              enum: ["any", "male", "female"]
        responses:
          '200':
            description: 'Return Bernallio Census data'
        """

        minage = int(self.request.rel_url.query.get('agemin', MIN_AGE))
        maxage = int(self.request.rel_url.query.get('agemax', MAX_AGE))
        gender = self.request.rel_url.query.get('gender', "any").lower()

        result = {}

        if all([minage, maxage, gender]):
            re_gender = re.compile(r'^({})$'.format(gender if gender != 'any' else 'female|male'), re.IGNORECASE)
            categories = await self.request.app._db.census_filters.find(
                {
                    'type': 'age',
                    'min': {'$gte': minage},
                    'max': {'$lte': maxage},
                    'gender': re_gender
                },
                {
                    '_id': 0,
                }
            ).to_list(length=None)
            result['categories'] = categories

            return_set = {
                '_id': 0,
                "GEOID": 1,
            }
            for category in result['categories']:
                filter_field = category["meta_index"] + "_with_ann_" + category["category"]
                return_set[filter_field] = 1

            query = await self.request.app._db.cities.find({}, return_set).to_list(length=None)
            result['filter'] = query
        return web.json_response(result, dumps=json_dumps)


class BernallioGeometriesHandler(web.View):
    """Bernallio geometries handler"""

    async def get(self) -> web.Response:
        """
        ---
        summary: 'Get Bernallio geometries'
        tags:
          - Census
        responses:
          '200':
            description: 'Return Bernallio geometries'
        """

        result = await self.request.app._db.geometries.find({}, {'_id': 0}).to_list(length=None)
        return web.json_response(result, dumps=json_dumps)