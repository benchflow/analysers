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
    
def createQuery(CassandraRDD, experimentID, containerID, hostID):
    from commons import computeExperimentMetrics, computeModeMinMax, computeMetrics
    
    metrics = computeExperimentMetrics(CassandraRDD, "ram")
    metrics.update(computeModeMinMax(CassandraRDD, "ram"))
    
    data = CassandraRDD.map(lambda x: x["ram_integral"]).collect()

    integralMetrics = computeMetrics(data)
    
    return [{"experiment_id":experimentID, "container_id":containerID, "host_id":hostID, "ram_mode_min":metrics["min"], "ram_mode_max":metrics["max"], \
              "ram_mode_min_freq":metrics["mode_min_freq"], "ram_mode_max_freq":metrics["mode_max_freq"], \
              "ram_median_min":metrics["median_min"], "ram_median_max":metrics["median_max"], \
              "ram_mean_min":metrics["mean_min"], "ram_mean_max":metrics["mean_max"], \
              "ram_min":metrics["min"], "ram_max":metrics["max"], "ram_q1_min":metrics["q1_min"], \
              "ram_q1_max":metrics["q1_max"], "ram_q2_min":metrics["q2_min"], "ram_q2_max":metrics["q2_max"], \
              "ram_p95_max":metrics["p95_max"], "ram_p95_min":metrics["p95_min"], \
              "ram_q3_min":metrics["q3_min"], "ram_q3_max":metrics["q3_max"], "ram_weighted_avg":metrics["weighted_avg"], \
              "ram_best": metrics["best"], "ram_worst": metrics["worst"], "ram_average": metrics["average"], \
              "ram_integral_median":integralMetrics["median"], "ram_integral_mean":integralMetrics["mean"], \
              "ram_integral_min":integralMetrics["min"], "ram_integral_max":integralMetrics["max"], "ram_integral_sd":integralMetrics["sd"], \
              "ram_integral_q1":integralMetrics["q1"], "ram_integral_q2":integralMetrics["q2"], "ram_integral_q3":integralMetrics["q3"], \
              "ram_integral_p95":integralMetrics["p95"], "ram_integral_me":integralMetrics["me"], \
              "ram_integral_ci095_min":integralMetrics["ci095_min"], "ram_integral_ci095_max":integralMetrics["ci095_max"]}]
    
def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments: Spark master, Cassandra host, Minio host, path of the files
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    containerID = sys.argv[4]
    hostID = sys.argv[5]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Ram analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "trial_ram"
    destTable = "exp_ram"
    
    CassandraRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], "trial_ram") \
        .select("ram_min", "ram_max", "ram_q1", "ram_q2", "ram_q3", "ram_p95", "ram_median", "ram_num_data_points", "ram_mean", "ram_me", "trial_id", "ram_integral", "ram_mode", "ram_mode_freq") \
        .where("experiment_id=? AND container_id=? AND host_id=?", experimentID, containerID, hostID)
    CassandraRDD.cache()
    
    query = createQuery(CassandraRDD, experimentID, containerID, hostID)

    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()