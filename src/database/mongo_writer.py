"""
High-throughput storage layer designed to bypass GridFS
Converts Spark DataFrames into standard BSON documents and executes bulk inserts.
"""

import pymongo
from typing import Any
import src.config as config

def write_partition_to_mongo(partition: Any, epoch_id: int) -> None:
    """
    Executes within individual Spark worker nodes.
    Opens a local connection to MongoDB and bulk-inserts the partition's records.
    """
    # 1 Initialize MongoDB client inside the worker to avoid serialization errors
    client = pymongo.MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB_NAME]
    collection = db[config.MONGO_COLLECTION_NAME]

    documents = []

    # 2. Convert Spark Rows into BSON-compatible Python dictionaries
    for row in partition:
        doc = row.asDict(recursive=True)
        # Attach the Spark streaming epoch ID for lineage tracking
        doc["batch_epoch_id"] = epoch_id
        documents.append(doc)

    # 3. Execute bulk inserts to optimize write-throughput metrics
    if documents:
        try:
            collection.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            print(f"Bulk write error in epoch {epoch_id}: {bwe.details}")

    client.close()

def spark_mongo_sink(df: Any, epoch_id: int) -> None:
    """
    The sink function to be passed into Spark's `foreachBatch`.
    Routes the distributed DataFrame to the worker-level write function.
    """
    df.repartition(50).foreachPartition(lambda partition: write_partition_to_mongo(partition, epoch_id))