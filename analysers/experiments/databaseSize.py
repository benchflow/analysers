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
    
    return [{"experiment_id":experimentID, "size_mode":mode[0], "size_mode_freq":mode[1], "size_median":metrics["median"], \
              "size_avg":metrics["mean"], "size_num_data_points":metrics["num_data_points"], \
              "size_min":metrics["min"], "size_max":metrics["max"], "size_sd":metrics["sd"], \
              "size_q1":metrics["q1"], "size_q2":metrics["q2"], "size_q3":metrics["q3"], "size_p95":metrics["p95"], \
              "size_me":metrics["me"], "size_ci095_min":metrics["ci095_min"], "size_ci095_max":metrics["ci095_max"]}]

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
    srcTable = "trial_byte_size"
    destTable = "exp_byte_size"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("size") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['size'] is not None) \
            .map(lambda r: (r['size'], 1)) \
            .cache()
            
    query = createQuery(dataRDD, experimentID)
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()