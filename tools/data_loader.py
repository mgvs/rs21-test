#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Data Loader"""

import argparse
import time
import datetime
import os
import json

import yaml
from pymongo import ASCENDING, GEOSPHERE

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from rs21_test.lib.db import DatabaseConfig


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
        def sentim(x):
            if x > 0: return 'positive'
            if x < 0: return 'negative'
            else: return 'neutral'

        new_objects = []
        for row in twitter_data:
            new_objects.append({
                'username': row[1],
                'tweet': row[0],
                'datetime': datetime.datetime.strptime(row[4].strip('\n').strip(';'), '%Y-%m-%d %H:%M:%S'),
                'location': {"type": "Point", "coordinates": [float(row[3]), float(row[2])]},
                'sentiment': sentim(analyser.polarity_scores(row[0])['compound'])
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
            result = {}

            metadata_files = filter(lambda x: x.endswith('csv'), os.listdir(self.cfg['LOADER']['DATA']['BERNALLIO']))
            for meta_filename in metadata_files:
                meta_index = meta_filename.split('_metadata')[0]
                if meta_index not in result:
                    result[meta_index] = {}
                with open(os.path.join(self.cfg['LOADER']['DATA']['BERNALLIO'], meta_filename), 'r', encoding='latin-1') as fd:
                    for line in fd.readlines():
                        l = line.replace('\n', '').split(',', 1)
                        if l[0] not in result[meta_index]:
                            result[meta_index][l[0]] = l[1]
            return result

        self._db.bernallio.drop()
        self._db.bernallio.create_index([("properties.location", GEOSPHERE)])
        self._db.bernallio.create_index([("geometry", GEOSPHERE)])

        mapper = prepare_mapper()

        new_objects = []
        json_files = filter(lambda x: x.endswith('json'), os.listdir(self.cfg['LOADER']['DATA']['BERNALLIO']))
        for json_filename in json_files:
            data = json.loads(open(os.path.join(self.cfg['LOADER']['DATA']['BERNALLIO'], json_filename), 'r').read())
            for item in data['features']:
                obj = {
                    'geometry': item['geometry'],
                    'properties': {}
                }

                geo = {}
                for k, v in item['properties'].items():
                    if k in ["INTPTLON", "INTPTLAT"]:
                        geo[k] = float(v)
                    else:
                        k = k.split("_with_ann_")
                        if len(k) == 2:
                            obj['properties'][k[0]] = {
                                'description': mapper[k[0]].get(k[1]),
                                'value': v
                            }
                        else:
                            obj['properties'][k[0]] = {
                                'value': v
                            }
                obj['properties']['location'] = {"type": "Point", "coordinates": [geo["INTPTLON"], geo["INTPTLAT"]]}

                new_objects.append(obj)
        self._db.bernallio.insert_many(new_objects)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', help='config file', required=True)
    args = parser.parse_args()

    start = int(time.time())
    app = DataLoader(args.config)

    # app.load_facebook()
    # app.load_twitter()
    app.load_bernallio()

    # app.load_bernallio_age()
    # app.load_bernallio_median_age()
    # app.load_bernallio_transportation()
    # app.load_bernallio_households()
    # app.load_bernallio_earnings()

    print("Finished in {} sec".format(int(time.time()) - start))
