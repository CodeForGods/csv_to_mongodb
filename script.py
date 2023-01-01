import pymongo
import csv


def mongo_connection_util(client_url: str, db: str, col: str):
    client = pymongo.MongoClient(client_url)
    database = client[db]
    collection = database[col]

    return collection


def insert_data(csv_data, collection):
    data = []
    with open(csv_data, 'r') as outf:
        for line in csv.DictReader(outf):
            data.append(line)

    collection.insert_many(data)
    print("Data Inserted Successfully")


if __name__ == "__main__":
    # mongodb connection url
    client = "mongodb+srv://<#$#$#$#$>:$#$#$#$##$#$#$#$#$#$#$$#@##$##@@##$$@#$#@#$#$#@#$#@#$#@#$#/test"

    # database name
    db = 'demo2'

    # collection name
    col = 'users'

    # file name ( you can also provide path)
    file = 'users_internet_usage.csv'

    try:
        mongo_collection = mongo_connection_util(client, db, col)

        insert_data(file, mongo_collection)
    except Exception as Ex:
        print("Exception Occurred : " + Ex)
