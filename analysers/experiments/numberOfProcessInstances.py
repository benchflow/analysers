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

def createQuery(dataRDD, experimentID, trialID):
    from commons import computeMode, computeMetrics
    
    mode = computeMode(dataRDD)

    data = dataRDD.map(lambda x: x[0]).collect()
     
    metrics = computeMetrics(data)
    
    return [{"experiment_id":experimentID, "number_of_process_instances_mode":mode[0], "number_of_process_instances_mode_freq":mode[1], "number_of_process_instances_median":metrics["median"], \
              "number_of_process_instances_avg":metrics["mean"], "number_of_process_instances_num_data_points":metrics["num_data_points"], \
              "number_of_process_instances_min":metrics["min"], "number_of_process_instances_max":metrics["max"], "number_of_process_instances_sd":metrics["sd"], \
              "number_of_process_instances_q1":metrics["q1"], "number_of_process_instances_q2":metrics["q2"], "number_of_process_instances_q3":metrics["q3"], "number_of_process_instances_p95":metrics["p95"], \
              "number_of_process_instances_me":metrics["me"], "number_of_process_instances_ci095_min":metrics["ci095_min"], "number_of_process_instances_ci095_max":metrics["ci095_max"]}]

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    trialID = sys.argv[1]
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of process instances analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "trial_number_of_process_instances"
    destTable = "exp_number_of_process_instances"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("number_of_process_instances") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['number_of_process_instances'] is not None) \
            .map(lambda r: (r['number_of_process_instances'], 1)) \
            .cache()
            
    query = createQuery(dataRDD, experimentID, trialID)
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()