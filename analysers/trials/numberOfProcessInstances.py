import sys
import json
import io
import gzip
import uuid

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def createQuery(sc, cassandraKeyspace, srcTable, experimentID, trialID, partitionsPerCore):
    queries = []
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("process_name", "source_process_instance_id", "to_ignore", "start_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["process_name"] is not None and r["to_ignore"] is False) \
            .repartition(sc.defaultParallelism * partitionsPerCore) \
            .cache()
    
    numberOfInstances = dataRDD.count()
    
    queries.append({"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":numberOfInstances, "process_definition_id": "all"})
    
    processes = dataRDD.map(lambda a: a["process_name"]).distinct().collect()
    
    for process in processes:
        numberOfInstances = dataRDD.filter(lambda a: a["process_name"] == process).count()
        queries.append({"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":numberOfInstances, "process_definition_id": process})
    
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    partitionsPerCore = 5
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of process instances analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "process"
    destTable = "trial_number_of_process_instances"
        
    query = createQuery(sc, cassandraKeyspace, srcTable, experimentID, trialID, partitionsPerCore)
    
    sc.parallelize(query, sc.defaultParallelism * partitionsPerCore).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__': main()