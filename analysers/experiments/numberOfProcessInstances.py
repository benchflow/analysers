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
    
    #Iterating over all process definitions
    for process in processes:
        mode = computeMode(dataRDD.filter(lambda a: a["process_definition_id"] == process).map(lambda r: (r['number_of_process_instances'], 1)))
    
        data = dataRDD.filter(lambda r: r['process_definition_id'] == process).map(lambda r: r['number_of_process_instances']).collect()
         
        metrics = computeMetrics(data)
        
        queries.append({"process_definition_id": process, "experiment_id":experimentID, "number_of_process_instances_mode":mode[0], "number_of_process_instances_mode_freq":mode[1], \
                  "number_of_process_instances_mean":metrics["mean"], "number_of_process_instances_num_data_points":metrics["num_data_points"], \
                  "number_of_process_instances_min":metrics["min"], "number_of_process_instances_max":metrics["max"], "number_of_process_instances_sd":metrics["sd"], "number_of_process_instances_variance":metrics["variance"], \
                  "number_of_process_instances_q1":metrics["q1"], "number_of_process_instances_q2":metrics["q2"], "number_of_process_instances_q3":metrics["q3"], "number_of_process_instances_p95":metrics["p95"], \
                  "number_of_process_instances_p90":metrics["p90"], "number_of_process_instances_p99":metrics["p99"], "number_of_process_instances_percentiles":metrics["percentiles"], \
                  "number_of_process_instances_me":metrics["me"], "number_of_process_instances_ci095_min":metrics["ci095_min"], "number_of_process_instances_ci095_max":metrics["ci095_max"]})
        
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of process instances analyser")
    sc = CassandraSparkContext(conf=conf)

    #Source and destination tables
    srcTable = "trial_number_of_process_instances"
    destTable = "exp_number_of_process_instances"
    
    #Retrieving data for computations
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("number_of_process_instances", "process_definition_id") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['number_of_process_instances'] is not None) \
            .cache()
    
    #Creating the Cassandra query
    query = createQuery(dataRDD, experimentID)
    
    #Save to Cassandra
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__':
    main()