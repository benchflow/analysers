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
    
def createQuery(sc, cassandraKeyspace, srcTable, dataTable, experimentID, containerName, hostID):
    from commons import computeExperimentMetrics, computeModeMinMax, computeMetrics, computeLevene, computeCombinedVar
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, "trial_ram") \
        .select("ram_min", "ram_max", "ram_q1", "ram_q2", "ram_q3", "ram_p90", "ram_p95", "ram_p99", "ram_num_data_points", "ram_mean", "ram_me", "trial_id", "ram_integral", "ram_mode", "ram_mode_freq", "ram_variance") \
        .where("experiment_id=? AND container_name=? AND host_id=?", experimentID, containerName, hostID)
    CassandraRDD.cache()
    
    metrics = computeExperimentMetrics(CassandraRDD, "ram")
    metrics.update(computeModeMinMax(CassandraRDD, "ram"))
    
    data = CassandraRDD.map(lambda x: x["ram_integral"]).collect()

    integralMetrics = computeMetrics(data)
    
    levenePValue = computeLevene(sc, cassandraKeyspace, srcTable, dataTable, experimentID, containerName, hostID, "memory_usage")
    
    combinedVar = computeCombinedVar(CassandraRDD, "ram")
    
    return [{"experiment_id":experimentID, "container_name":containerName, "host_id":hostID, "ram_mode_min":metrics["min"], "ram_mode_max":metrics["max"], \
              "ram_mode_min_freq":metrics["mode_min_freq"], "ram_mode_max_freq":metrics["mode_max_freq"], \
              "ram_mean_min":metrics["mean_min"], "ram_mean_max":metrics["mean_max"], \
              "ram_min":metrics["min"], "ram_max":metrics["max"], "ram_q1_min":metrics["q1_min"], \
              "ram_q1_max":metrics["q1_max"], "ram_q2_min":metrics["q2_min"], "ram_q2_max":metrics["q2_max"], \
              "ram_p90_max":metrics["p90_max"], "ram_p90_min":metrics["p90_min"], \
              "ram_p95_max":metrics["p95_max"], "ram_p95_min":metrics["p95_min"], \
              "ram_p99_max":metrics["p99_max"], "ram_p95_min":metrics["p99_min"], \
              "ram_q3_min":metrics["q3_min"], "ram_q3_max":metrics["q3_max"], "ram_weighted_avg":metrics["weighted_avg"], \
              "ram_best": metrics["best"], "ram_worst": metrics["worst"], "ram_average": metrics["average"], \
              "ram_integral_mean":integralMetrics["mean"], \
              "ram_integral_min":integralMetrics["min"], "ram_integral_max":integralMetrics["max"], "ram_integral_sd":integralMetrics["sd"], \
              "ram_integral_q1":integralMetrics["q1"], "ram_integral_q2":integralMetrics["q2"], "ram_integral_q3":integralMetrics["q3"], \
              "ram_integral_p95":integralMetrics["p95"], "ram_integral_me":integralMetrics["me"], \
              "ram_integral_ci095_min":integralMetrics["ci095_min"], "ram_integral_ci095_max":integralMetrics["ci095_max"], \
              "ram_levene_test_mean":levenePValue["levene_mean"], "ram_levene_test_median":levenePValue["levene_median"], "ram_levene_test_trimmed":levenePValue["levene_trimmed"], \
              "ram_levene_test_mean_stat":levenePValue["levene_mean_stat"], "ram_levene_test_median_stat":levenePValue["levene_median_stat"], "ram_levene_test_trimmed_stat":levenePValue["levene_trimmed_stat"], \
              "ram_variation_coefficient": metrics["variation_coefficient"], "ram_combined_variance": combinedVar}]
    
def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    containerName = str(args["container_name"])
    hostID = str(args["host_id"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Ram analyser")
    sc = CassandraSparkContext(conf=conf)

    cassandraKeyspace = "benchflow"
    dataTable = "environment_data"
    srcTable = "trial_ram"
    destTable = "exp_ram"
    
    query = createQuery(sc, cassandraKeyspace, srcTable, dataTable, experimentID, containerName, hostID)

    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__':
    main()