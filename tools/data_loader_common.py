#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Data Loader"""

import logging
import argparse
import csv
import time
import datetime
import glob
import sys

import yaml
from pymongo import ASCENDING, GEOSPHERE

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

    def _get_twitter(self):
        twitter_dir = self.cfg['DATA']['TWITTER']['DIR']
        twitter_header = self.cfg['DATA']['TWITTER']['HEADER']
        twitter_delimiter = self.cfg['DATA']['TWITTER']['DELIMETER']
        twitter_files = glob.glob(twitter_dir + '*.csv')
        result = []
        for file_name in twitter_files:
            with open(file_name, 'r') as fd:
                reader = csv.DictReader(fd, fieldnames=twitter_header, delimiter=twitter_delimiter)
                result += [dict(row) for row in reader]
        return result

    def _get_facebook(self):
        facebook_dir = self.cfg['DATA']['FACEBOOK']['DIR']
        facebook_delimiter = self.cfg['DATA']['FACEBOOK']['DELIMETER']
        facebook_files = glob.glob(facebook_dir + '*.csv')
        for file_name in facebook_files:          
            with open(file_name, 'r', encoding='latin-1') as fd:
                 reader = csv.reader(fd, delimiter=facebook_delimiter)
                 result = [list(filter(None, row)) for row in reader]
        return result

    def _get_bernallio_age(self):
        bernallio_age_file = self.cfg['DATA']['BERNALLIO']['FILES']['AGE']
#        bernallio_age_files = glob.glob(bernallio_age_dir + '*.csv')
#        for file_name in bernallio_age_files:

        with open(bernallio_age_file, 'r', encoding='latin-1') as fd:
            result = [row.replace(': - ', ',').replace('; ', ',').strip().split(',') for row in fd if 'Estimate' in row]
        return result


    def _get_bernallio_median_age(self):
        bernallio_median_age_dir = self.cfg['DATA']['BERNALLIO']['FILES']['MEDIAN_AGE']
        bernallio_median_age_files = glob.glob(bernallio_median_age_dir + '*.csv')
        for file_name in bernallio_median_age_files:
            with open(file_name, 'r', encoding='latin-1') as fd:
                result = [row.replace(' -- - ', ',').replace('; ', ',').strip().split(',') for row in fd if 'Estimate' in row]
        return result

    def _get_bernallio_transportation(self):
        bernallio_transportation_dir = self.cfg['DATA']['BERNALLIO']['FILES']['TRANSPORTATION']
        bernallio_transportation_files = glob.glob(bernallio_transportation_dir + '*.csv')
        for file_name in bernallio_transportation_files:
            with open(file_name, 'r', encoding='latin-1') as fd:
                result = [row.replace(',', ' ').replace('"', '').strip().split() for row in fd if 'Estimate' in row]
        return result

    def _get_bernallio_households(self):
        bernallio_households_dir = self.cfg['DATA']['BERNALLIO']['FILES']['HOUSEHOLD']
        bernallio_households_files = glob.glob(bernallio_households_dir + '*.csv')
        for file_name in bernallio_households_files:
            with open(file_name, 'r', encoding='latin-1') as fd:
                result = [row.replace(';', ',').replace('"', '').strip().split(',') for row in fd if 'Estimate' in row]
        return result

    def _get_bernallio_earnings(self):
        bernallio_earnings_dir = self.cfg['DATA']['BERNALLIO']['FILES']['EARNINGS']
        bernallio_earnings_files = glob.glob(bernallio_earnings_dir + '*.csv')
        for file_name in bernallio_earnings_files:
            with open(file_name, 'r', encoding='latin-1') as fd:
                result = [row.replace(';', ',').replace('"', '').strip().split(',') for row in fd if 'Estimate' in row]
        return result


    def load_bernallio_engin(self):
        self._db.bernallio.drop()
        self._db.bernallio.create_index([('id', ASCENDING)])


    def load_bernallio_earnings(self):
        for row in self._get_bernallio_earnings():
            obj = {
                  'id': '{}_{}'.format('ACS_13_5YR_B19051_with_ann', row[0]),
                  'earnings': ' '.join(row[2:])
            }
            self._db.bernallio.insert_one(obj)

    def load_bernallio_households(self):
        for row in self._get_bernallio_households():
            obj = {
                  'id': '{}_{}'.format('ACS_13_5YR_B11001_with_ann', row[0]),
                  'households': ' '.join(row[2:])
            }
            self._db.bernallio.insert_one(obj)

    def load_bernallio_transportation(self):
        for row in self._get_bernallio_transportation():
            obj = {
                  'id': '{}_{}'.format('ACS_13_5YR_B08301_with_ann', row[0]),
                  'transportation': ' '.join(row[2:])
            }
            self._db.bernallio.insert_one(obj)

    def load_bernallio_median_age(self):
        for row in self._get_bernallio_median_age():
            obj = {
                  'id': '{}_{}'.format('ACS_13_5YR_B01002_with_ann', row[0]),
                  'median_age': row[-1]
            }
            self._db.bernallio.insert_one(obj)

    def load_bernallio_age(self):
        for row in self._get_bernallio_age():
            obj = {
                  'id': '{}_{}'.format('ACS_13_5YR_B01001_with_ann', row[0]),
                  'sex': row[-2], 
                  'age': row[-1]
            }
            self._db.bernallio.insert_one(obj)


    def load_twitter(self):
        self._db.twitter.drop()
        self._db.twitter.create_index([("username", ASCENDING)])
        self._db.twitter.create_index([("twit", ASCENDING)])
        self._db.twitter.create_index([("time", ASCENDING)])
        self._db.twitter.create_index([("location", GEOSPHERE)])
        for row in self._get_twitter():
            obj = {
                  'username': row['username'],
                  'twit': row['tweet'],
                  'datetime': row['time'],
                  'location': {"type": "Point", "coordinates": [float(row['lon']), float(row['lat'])]}
            }
            self._db.twitter.insert_one(obj)

    def load_facebook(self):
        self._db.facebook.drop()
        self._db.facebook.create_index([("place", ASCENDING)])
        self._db.facebook.create_index([("bus_type", ASCENDING)])
        self._db.facebook.create_index([("checkins", ASCENDING)])
        self._db.facebook.create_index([("location", GEOSPHERE)])
        for row in self._get_facebook():
            obj = {'place': row[0],
                   'bus_type': row[-4],
                   'checkins': row[-3],
                   'location': {"type": "Point", "coordinates": [float(row[-1]), float(row[-2])]}
            }
            self._db.facebook.insert_one(obj)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', help='config file', required=True)
    args = parser.parse_args()

    start = int(time.time())
    app = DataLoader(args.config)

    app.load_facebook()
    app.load_twitter()

    app.load_bernallio_engin()
    app.load_bernallio_age()
#    app.load_bernallio_median_age()
#    app.load_bernallio_transportation()
#    app.load_bernallio_households()
#    app.load_bernallio_earnings()

    print("Finished in {} sec".format(int(time.time()) - start))
