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

def createQuery(dataRDD, experimentID, trialID, containerID, hostID):
    from commons import computeMode, computeMetrics
    
    mode = computeMode(dataRDD)
    
    data = dataRDD.map(lambda x: x[0]).collect()
     
    metrics = computeMetrics(data)
    
    query = [{"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "host_id":hostID, \
              "ram_mode":mode[0], "ram_mode_freq":mode[1], "ram_median":metrics["median"], "ram_integral":metrics["integral"], \
              "ram_mean":metrics["mean"], "ram_num_data_points":metrics["num_data_points"], \
              "ram_min":metrics["min"], "ram_max":metrics["max"], "ram_sd":metrics["sd"], \
              "ram_q1":metrics["q1"], "ram_q2":metrics["q2"], "ram_q3":metrics["q3"], "ram_p95":metrics["p95"], \
              "ram_me":metrics["me"], "ram_ci095_min":metrics["ci095_min"], "ram_ci095_max":metrics["ci095_max"]}]
    return query

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    #Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Ram trial analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "environment_data"
    destTable = "trial_ram"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("memory_usage") \
            .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
            .filter(lambda r: r["memory_usage"] is not None) \
            .map(lambda r: (r['memory_usage'], 1)) \
            .cache()
    
    query = createQuery(dataRDD, experimentID, trialID, containerID, hostID)
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()