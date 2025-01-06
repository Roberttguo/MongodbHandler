import datetime
import os
import threading
import random
import time
from common import constant
from pymongo import MongoClient, ReturnDocument

from common.constant import mongo_url, collection_fn, BASE_COLLECTION
from common.logdecorator import slf4j

file_access_lock = threading.Lock()


@slf4j
class MyThread(threading.Thread):

    def __init__(self, arg):
        super().__init__()
        self.arg = arg
        self.mongodb_connection = MongoClient(mongo_url)
        self.log.info("db name:" + arg["dbname"])
        self.db = self.mongodb_connection[arg["dbname"]]

    def does_mongodb_collection_exist(self, collection):
        try:
            collections = self.db.list_collections()
            mylist = collections.to_list()
            self.log.info("my col list: " + str(mylist))
            names = self.db.list_collection_names()
            self.log.info("Collections names: " + str(names))  #, dir(collections))
            return True if collection in names else False
        except Exception as e:
            self.log.error("Exception: " + str(e))
            return False

    def does_dbname_exist(self, dbname):
        dbnames = self.mongodb_connection.list_database_names()
        return dbname in dbnames

    def create_db(self, dbname):
        """
        :param dbname: strin, dbname in connected mongodb server
        :return:
        """
        if not self.does_dbname_exist(dbname):
            self.db = self.mongodb_connection[dbname]

    def create_collection(self, collection, addfirst=False):
        """
        :param addfirst: boolean. add first element if True
        :param collection: string
        :return: collectin obj if success, None otherwise
        """
        #if not self.db:
        #    return None
        if not self.does_mongodb_collection_exist(collection):
            col = self.db.create_collection(collection)
            myfirst = {"name": "first", "address": "Highway 37"}
            if addfirst:
                col.insert_one(myfirst)
            return col
        else:
            return self.db.get_collection(collection)

    def _append_collection(self):
        names = ["smith", "Trump", "Musk", "Biden", "Alberto", "Yo"]
        ages = [20, 70, 50, 33]
        with file_access_lock:
            with open(collection_fn, "a+") as f:
                line = "name %s age %d" % (random.choice(names), random.choice(ages))
                f.writelines("{}\n".format(line))

    def _insert_to_collection(self, col):
        """
        :param col: object, mongodb collection instance
        :return:
        """
        with file_access_lock:
            with open(collection_fn, "r+t") as f:
                lines = f.readlines()
                for line in lines:
                    items = line.split(" ")
                    count = len(items)
                    if count % 2 != 0:  #not even
                        raise "The pairs for key and value do not match."
                    i = 0
                    entry = {}
                    while i < count:
                        key = items[i]
                        value = items[i + 1]
                        entry[key] = value
                        i += 2
                    col.insert_one(entry)

    def push_to_collection(self, collection):
        col = self.db.get_collection(collection)
        self.log.debug("collection: " + str(col))
        if col.name != collection:
            self.log.error("collection {} not found" % collection)
            raise "collection {} not found" % collection
        #dice
        dice = [True, False]
        selection = random.choice(dice)

        if selection:
            self.log.info("insert entries to collection.")
            self._insert_to_collection(col)
        else:
            self.log.info("append new entries to collection file.")
            self._append_collection()

    def run(self):
        cl = self.create_collection(BASE_COLLECTION)
        cl.insert_one({"name": "new_" + str(datetime.datetime.now())})
        res = cl.find_one_and_update({"name": "John"}, {'$set': {"Branch": "private"}},
                                     return_document=ReturnDocument.AFTER)
        doc_count = cl.find()
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=self.push_to_collection, args=(BASE_COLLECTION,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

        #self.push_to_collection(BASE_COLLECTION)
        self.mongodb_connection.close()


cl = MyThread({"dbname": "python_created"})
cl.run()
