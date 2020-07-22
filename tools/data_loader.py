#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Data Loader"""

import argparse
import time
import datetime
import os
import json
import re

import yaml
from pymongo import ASCENDING, GEOSPHERE

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from rs21_test.lib.db import DatabaseConfig
from rs21_test.app.handlers.bernallio import MIN_AGE, MAX_AGE


class DataLoader:

    def __init__(self, config_file_path):
        with open(config_file_path, 'r') as fd:
            self.cfg = yaml.safe_load(fd)

        self._db = DatabaseConfig.pymongo(
            self.cfg['MONGO_DB']['HOST'],
            self.cfg['MONGO_DB']['PORT'],
            self.cfg['MONGO_DB']['DB_NAME'],
        )

    def load_twitter(self):
        """
        Load Twitter entries
        """

        def get_sentim(x: int) -> int:
            if x > 0:
                return 1  # positive
            if x < 0:
                return -1  # negative
            return 0  # neutral

        self._db.twitter.drop()
        self._db.twitter.create_index([("username", ASCENDING)])
        self._db.twitter.create_index([("twit", ASCENDING)])
        self._db.twitter.create_index([("time", ASCENDING)])
        self._db.twitter.create_index([("location", GEOSPHERE)])
        self._db.twitter.create_index([("sentiment", ASCENDING)])

        twitter_data = []
        twitter_files = filter(lambda x: x.endswith('csv'), os.listdir(self.cfg['LOADER']['DATA']['TWITTER']))
        for file_name in map(lambda x: os.path.join(self.cfg['LOADER']['DATA']['TWITTER'], x), twitter_files):
            with open(file_name, 'r', encoding='latin-1') as fd:
                twitter_data = list(filter(lambda x: len(x) == 5, [x.rsplit(',', 4) for x in fd.readlines()]))
                
        analyser = SentimentIntensityAnalyzer()

        new_objects = []
        for row in twitter_data:
            new_objects.append({
                'username': row[1],
                'tweet': row[0],
                'datetime': datetime.datetime.strptime(row[4].strip('\n').strip(';'), '%Y-%m-%d %H:%M:%S'),
                'location': {"type": "Point", "coordinates": [float(row[3]), float(row[2])]},
                'sentiment': get_sentim(analyser.polarity_scores(row[0])['compound'])
            })
        self._db.twitter.insert_many(new_objects)

    def load_facebook(self):
        """
        Load Facebook entries
        """
        self._db.facebook.drop()
        self._db.facebook.create_index([("place", ASCENDING)])
        self._db.facebook.create_index([("type", ASCENDING)])
        self._db.facebook.create_index([("location", GEOSPHERE)])

        facebook_data = []
        facebook_files = filter(lambda x: x.endswith('csv'), os.listdir(self.cfg['LOADER']['DATA']['FACEBOOK']))
        for file_name in map(lambda x: os.path.join(self.cfg['LOADER']['DATA']['FACEBOOK'], x), facebook_files):
            with open(file_name, 'r', encoding='latin-1') as fd:
                facebook_data = list(filter(lambda x: len(x) == 5, [x.rsplit(',', 4) for x in [n.replace('\n', '').rstrip(',,,') for n in fd.readlines()]]))

        new_objects = []
        for row in facebook_data:
            new_objects.append({
                'place': row[0],
                'type': row[1],
                'checkins': int(row[2]),
                'location': {"type": "Point", "coordinates": [float(row[4]), float(row[3])]}
            })

        self._db.facebook.insert_many(new_objects)

    def load_bernallio(self):
        """
        Load Bernallio entries
        """
        def prepare_mapper():
            re_subtype_gender = re.compile(r'^(Estimate|Margin\sof\sError).*(Female|Male).*?$')
            re_age_range = re.compile(r'\d+')

            result = {}
            filters = []

            metadata_files = filter(lambda x: x.endswith('csv'), os.listdir(self.cfg['LOADER']['DATA']['BERNALLIO']))
            for meta_filename in metadata_files:
                meta_index = meta_filename.split('_metadata')[0]
                if meta_index not in result:
                    result[meta_index] = {}
                with open(os.path.join(self.cfg['LOADER']['DATA']['BERNALLIO'], meta_filename), 'r', encoding='latin-1') as fd:
                    for line in fd.readlines():
                        category, description = line.replace('\n', '').split(',', 1)
                        if category not in result[meta_index]:
                            result[meta_index][category] = description

                        # Age/Gender filters
                        if 'B01001' in meta_index:
                            subtype_gender = re_subtype_gender.findall(description)
                            age_range = re_age_range.findall(description)

                            min_age = MIN_AGE
                            max_age = MAX_AGE

                            if len(age_range) == 2:
                                min_age, max_age = age_range
                            elif len(age_range) == 1:
                                if 'over' in description.lower():
                                    min_age = age_range[0]
                                elif 'under' in description.lower():
                                    max_age = age_range[0]
                                else:
                                    min_age = age_range[0]
                                    max_age = age_range[0]

                            if subtype_gender and age_range:
                                filters.append({
                                    'type': 'age',
                                    'category': category,
                                    'subtype': subtype_gender[0][0],
                                    'gender': subtype_gender[0][1],
                                    'min': int(min_age),
                                    'max': int(max_age),
                                    'meta_index': meta_index,
                                })
            self._db.census_filters.insert_many(filters)
            return result

        self._db.cities.drop()
        self._db.cities.create_index([("location", GEOSPHERE)])

        self._db.census_filters.drop()
        self._db.census_filters.create_index([("type", ASCENDING)])
        self._db.census_filters.create_index([("type", ASCENDING), ('min', ASCENDING), ('max', ASCENDING)])
        self._db.census_filters.create_index([("type", ASCENDING), ('min', ASCENDING), ('max', ASCENDING), ('gender', ASCENDING)])

        self._db.geometries.drop()
        self._db.geometries.create_index([("GEOID", ASCENDING)])
        self._db.geometries.create_index([("geometry", GEOSPHERE)])

        mapper = prepare_mapper()

        geometries = []
        new_objects = []
        json_files = filter(lambda x: x.endswith('json'), os.listdir(self.cfg['LOADER']['DATA']['BERNALLIO']))
        for json_filename in json_files:
            data = json.loads(open(os.path.join(self.cfg['LOADER']['DATA']['BERNALLIO'], json_filename), 'r').read())
            for item in data['features']:
                new_geometry = {
                    'geometry': item['geometry']
                }

                new_obj = {
                    'city': "Bernallio",
                }

                geo = {}
                for k, v in item['properties'].items():
                    k = k.replace('.', '-')
                    if k == "GEOID":
                        new_obj[k] = v
                        new_geometry[k] = v

                    if k in ["INTPTLON", "INTPTLAT"]:
                        geo[k] = float(v)
                    else:
                        new_obj[k] = v
                        # k_ = k.split("_with_ann_")
                        # if len(k_) == 2:
                        #     new_obj[k.replace('.', '-')] = {
                        #         'description': mapper[k_[0]].get(k_[1]),
                        #         'value': v
                        #     }
                        # else:
                        #     new_obj[k_[0]] = {
                        #         'value': v
                        #     }
                new_obj['location'] = {"type": "Point", "coordinates": [geo["INTPTLON"], geo["INTPTLAT"]]}

                geometries.append(new_geometry)
                new_objects.append(new_obj)

        self._db.cities.insert_many(new_objects)
        self._db.geometries.insert_many(geometries)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', help='config file', required=True)
    args = parser.parse_args()

    start = int(time.time())
    app = DataLoader(args.config)

    app.load_facebook()
    app.load_twitter()
    app.load_bernallio()

    print("Finished in {} sec".format(int(time.time()) - start))
