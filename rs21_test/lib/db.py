#!/apps/miniconda3/envs/rs21/bin/python3

import sys
import logging

import motor.motor_asyncio
from pymongo import MongoClient


class DatabaseConfig:

    @staticmethod
    def asyncmongo(host, port, db_name):
        try:
            conn = motor.motor_asyncio.AsyncIOMotorClient("mongodb://{}:{}".format(host, port))
        except Exception as e:
            logging.error("Unable to connect to mongodb, {}".format(e))
        else:
            logging.info("Connected to mongodb: {}".format(db_name))
            return conn[db_name]

    @staticmethod
    def pymongo(host, port, db_name, w=1, j=True):
        try:
            conn = MongoClient(
                host=host,
                port=int(port),
                maxPoolSize=8000,
                w=w,
            )
        except Exception as e:
            logging.critical("Unable to connect to mongodb (pymongo driver), {}".format(e))
            sys.exit(2)
        else:
            return conn[db_name]