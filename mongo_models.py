from pymongo import MongoClient
import config

model = MongoClient(config.db_url)

def number():
    return model[config.db_name].numbers