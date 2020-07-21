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
import json

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


    def _get_bernallio_joined(self):
        self._db.bern.drop()

        bernallio_joined_file = self.cfg['DATA']['BERNALLIO']['DIR']['JOINED']
        with open(bernallio_joined_file, 'r', encoding='latin-1') as fd:
            data = json.load(fd)['features']
            for item in data:
                block = {}
                for key, value in item['properties'].items():
                    block[key] = {'value': value}
                    result = self._db.bernallio.find_one({'id': key}, {'_id': 0, 'id': 0})
                    if result:
                        block.update({key: result})
         
                    print(block)
#                    self._db.bern.insert_one(block)
                    break
                



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', help='config file', required=True)
    args = parser.parse_args()

    start = int(time.time())
    app = DataLoader(args.config)
    
    app._get_bernallio_joined()

    print("Finished in {} sec".format(int(time.time()) - start))
