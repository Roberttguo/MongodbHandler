import unittest
import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MyTestCase:
    @classmethod
    def setupClass(cls):
        cls.client =MongoClient('localhost',27017, serverSelectionTimeoutMS=1000)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()


    def test_mongodb_connection(self):
        try:
            # The ismaster command is cheap and does not require auth
            self.client.admin.command('ismaster')
            self.assertTrue(True)
        except ConnectionFailure:
            self.fail("Server not available")
        #self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    pytest.main([__file__, "-v", f"::MyTestCase::test_mongodb_connection"])
