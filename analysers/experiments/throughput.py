import sys
import json
import io
import gzip
import uuid
import math

from datetime import timedelta

import scipy.integrate as integrate
import scipy.special as special
import numpy as np

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

#Create the queries containg the results of the computations to pass to Cassandra
def createQuery(dataRDD, experimentID):
    from commons import computeMode, computeMetrics
    
    queries = []
    
    processes = dataRDD.map(lambda a: a["process_definition_id"]).distinct().collect()
    
    #Iterate over all process definitions
    for process in processes:
        mode = computeMode(dataRDD.filter(lambda a: a["process_definition_id"] == process).map(lambda r: (r['throughput'], 1)))
    
        data = dataRDD.filter(lambda r: r['process_definition_id'] == process).map(lambda r: r['throughput']).collect()
         
        metrics = computeMetrics(data)
        
        queries.append({"process_definition_id": process, "experiment_id":experimentID, "throughput_mode":mode[0], "throughput_mode_freq":mode[1], \
                  "throughput_mean":metrics["mean"], "throughput_num_data_points":metrics["num_data_points"], \
                  "throughput_min":metrics["min"], "throughput_max":metrics["max"], "throughput_sd":metrics["sd"], "throughput_variance":metrics["variance"], \
                  "throughput_q1":metrics["q1"], "throughput_q2":metrics["q2"], "throughput_q3":metrics["q3"], "throughput_p95":metrics["p95"], \
                  "throughput_p90":metrics["p90"], "throughput_p99":metrics["p99"], "throughput_percentiles":metrics["percentiles"], \
                  "throughput_me":metrics["me"], "throughput_ci095_min":metrics["ci095_min"], "throughput_ci095_max":metrics["ci095_max"]})
        
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Throughput analyser")
    sc = CassandraSparkContext(conf=conf)

    #Source and destination tables
    srcTable = "trial_throughput"
    destTable = "exp_throughput"
    
    #Retrieving data for computations
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("throughput", "process_definition_id") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['throughput'] is not None) \
            .cache()
    
    #Create Cassandra query     
    query = createQuery(dataRDD, experimentID)
    
    #Save to Cassandra
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__':
    main()