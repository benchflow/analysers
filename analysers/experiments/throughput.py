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

def createQuery(dataRDD, experimentID):
    from commons import computeMode, computeMetrics
    
    mode = computeMode(dataRDD)

    data = dataRDD.map(lambda x: x[0]).collect()
     
    metrics = computeMetrics(data)
    
    return [{"experiment_id":experimentID, "throughput_mode":mode[0], "throughput_mode_freq":mode[1], "throughput_median":metrics["median"], \
              "throughput_avg":metrics["mean"], "throughput_num_data_points":metrics["num_data_points"], \
              "throughput_min":metrics["min"], "throughput_max":metrics["max"], "throughput_sd":metrics["sd"], \
              "throughput_q1":metrics["q1"], "throughput_q2":metrics["q2"], "throughput_q3":metrics["q3"], "throughput_p95":metrics["p95"], \
              "throughput_me":metrics["me"], "throughput_ci095_min":metrics["ci095_min"], "throughput_ci095_max":metrics["ci095_max"]}]

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of process instances analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "trial_throughput"
    destTable = "exp_throughput"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("throughput") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['throughput'] is not None) \
            .map(lambda r: (r['throughput'], 1)) \
            .cache()
            
    query = createQuery(dataRDD, experimentID)
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()