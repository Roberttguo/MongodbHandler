import os


mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017/")
collection_fn = os.environ.get("COLLECTION_ENTRY_FILE",
                          os.getcwd() + "/../source/collections.txt")
BASE_COLLECTION = os.environ.get("BASE_COLLECTION",
                          "Mongodb_base_collection")