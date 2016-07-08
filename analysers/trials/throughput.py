import sys
import json
import io
import gzip
import uuid
import math
import datetime 

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf
from pyspark import SparkFiles

def createQuery(sc, cassandraKeyspace, experimentID, trialID):
    queries = []
    
    execTimes = sc.cassandraTable(cassandraKeyspace, "trial_execution_time")\
            .select("process_definition_id", "execution_time") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .cache()
    numProcesses = sc.cassandraTable(cassandraKeyspace, "trial_number_of_process_instances")\
            .select("process_definition_id", "number_of_process_instances") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .cache()
    
    ex = execTimes.filter(lambda r: r["process_definition_id"] == "all").first()["execution_time"]
    npr = numProcesses.filter(lambda r: r["process_definition_id"] == "all").first()["number_of_process_instances"]
    
    tp = npr/(ex*1.0)
    
    queries.append({"experiment_id":experimentID, "trial_id":trialID, "process_definition_id":"all", "throughput":tp})
    
    processes = execTimes.map(lambda a: a["process_definition_id"]).distinct().collect()
    
    for process in processes:
        npr = numProcesses.filter(lambda r: r["process_definition_id"] == process).first()["number_of_process_instances"]
        
        tp = npr/(ex*1.0)
    
        queries.append({"experiment_id":experimentID, "trial_id":trialID, "process_definition_id":process, "throughput":tp})
        
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Process throughput trial analyser")
    sc = CassandraSparkContext(conf=conf)
    
    destTable = "trial_throughput"
            
    query = createQuery(sc, cassandraKeyspace, experimentID, trialID)
    
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()