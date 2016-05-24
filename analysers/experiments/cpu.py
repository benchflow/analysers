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

def sortAndGetCore(CassandraRDD, field, asc, i):
        if asc == 1:
            v = CassandraRDD.map(lambda x: x[field][i]) \
                .min()
        else:
            v = CassandraRDD.map(lambda x: x[field][i]) \
                .max()
        return v

def computeExperimentCoreMetrics(CassandraRDD, i):
    if CassandraRDD.isEmpty():
        return {"median_min":None, "median_max":None, \
              "min":None, "max":None, "q1_min":None, \
              "q1_max":None, "q2_min":None, "q2_max":None, \
              "p95_max":None, "p95_min":None, \
              "q3_min":None, "q3_max":None, "weighted_avg":None}
    
    dataMin = sortAndGetCore(CassandraRDD, "cpu_min", 1, i)
    dataMax = sortAndGetCore(CassandraRDD, "cpu_max", 0, i)
    q1Min = sortAndGetCore(CassandraRDD, "cpu_q1", 1, i)
    q1Max = sortAndGetCore(CassandraRDD, "cpu_q1", 0, i)
    q2Min = sortAndGetCore(CassandraRDD, "cpu_q2", 1, i)
    q2Max = sortAndGetCore(CassandraRDD, "cpu_q2", 0, i)
    q3Min = sortAndGetCore(CassandraRDD, "cpu_q3", 1, i)
    q3Max = sortAndGetCore(CassandraRDD, "cpu_q3", 0, i)
    p95Min = sortAndGetCore(CassandraRDD, "cpu_p95", 1, i)
    p95Max = sortAndGetCore(CassandraRDD, "cpu_p95", 0, i)
    medianMin = sortAndGetCore(CassandraRDD, "cpu_median", 1, i)
    medianMax = sortAndGetCore(CassandraRDD, "cpu_median", 0, i)
    
    someMeanIsNull = CassandraRDD.map(lambda x: x["cpu_mean"][i] is None) \
        .reduce(lambda a, b: a or b)
        
    if someMeanIsNull:
        weightedMean = None
    else:
        weightSum = CassandraRDD.map(lambda x: x["cpu_num_data_points"]) \
            .reduce(lambda a, b: a+b)
            
        weightedSum = CassandraRDD.map(lambda x: x["cpu_mean"][i]*x["cpu_num_data_points"]) \
            .reduce(lambda a, b: a+b)
        
        weightedMean = weightedSum/float(weightSum)
    
    return {"median_min":medianMin, "median_max":medianMax, \
              "min":dataMin, "max":dataMax, "q1_min":q1Min, \
              "q1_max":q1Max, "q2_min":q2Min, "q2_max":q2Max, \
              "p95_max":p95Max, "p95_min":p95Min, \
              "q3_min":q3Min, "q3_max":q3Max, "weighted_avg":weightedMean}
 
def createQuery(CassandraRDD, experimentID, containerID, hostID, nOfActiveCores):
    from commons import computeExperimentMetrics, computeMetrics
    
    metrics = computeExperimentMetrics(CassandraRDD, "cpu")
    
    data = CassandraRDD.map(lambda x: x["cpu_integral"]).collect()

    integralMetrics = computeMetrics(data)
    
    return [{"experiment_id":experimentID, "container_id":containerID, "host_id":hostID, "cpu_cores":nOfActiveCores, \
              "cpu_median_min":metrics["median_min"], "cpu_median_max":metrics["median_max"], \
              "cpu_min":metrics["min"], "cpu_max":metrics["max"], "cpu_q1_min":metrics["q1_min"], \
              "cpu_q1_max":metrics["q1_max"], "cpu_q2_min":metrics["q2_min"], "cpu_q2_max":metrics["q2_max"], \
              "cpu_p95_max":metrics["p95_max"], "cpu_p95_min":metrics["p95_min"], \
              "cpu_q3_min":metrics["q3_min"], "cpu_q3_max":metrics["q3_max"], "cpu_weighted_avg":metrics["weighted_avg"], \
              "cpu_best": metrics["best"], "cpu_worst": metrics["worst"], "cpu_average": metrics["average"], \
              "cpu_integral_median":integralMetrics["median"], "cpu_integral_avg":integralMetrics["mean"], \
              "cpu_integral_min":integralMetrics["min"], "cpu_integral_max":integralMetrics["max"], "cpu_integral_sd":integralMetrics["sd"], \
              "cpu_integral_q1":integralMetrics["q1"], "cpu_integral_q2":integralMetrics["q2"], "cpu_integral_q3":integralMetrics["q3"], \
              "cpu_integral_p95":integralMetrics["p95"], "cpu_integral_me":integralMetrics["me"], \
              "cpu_integral_ci095_min":integralMetrics["ci095_min"], "cpu_integral_ci095_max":integralMetrics["ci095_max"]}]
    
def createCoreQuery(CassandraRDD, experimentID, containerID, hostID, nOfActiveCores):
    nOfCores = len(CassandraRDD.first()["cpu_mean"])
    
    query = [{"experiment_id":experimentID, "container_id":containerID, "host_id":hostID, "cpu_cores":nOfActiveCores, \
              "cpu_median_min":[None]*nOfCores, "cpu_median_max":[None]*nOfCores, \
              "cpu_min":[None]*nOfCores, "cpu_max":[None]*nOfCores, "cpu_q1_min":[None]*nOfCores, \
              "cpu_q1_max":[None]*nOfCores, "cpu_q2_min":[None]*nOfCores, "cpu_q2_max":[None]*nOfCores, \
              "cpu_p95_max":[None]*nOfCores, "cpu_p95_min":[None]*nOfCores, \
              "cpu_q3_min":[None]*nOfCores, "cpu_q3_max":[None]*nOfCores, "cpu_weighted_avg":[None]*nOfCores}]
    
    for i in range(nOfCores):
        coreMetrics = computeExperimentCoreMetrics(CassandraRDD, i)
        
        query[0]["cpu_weighted_avg"][i] = coreMetrics["weighted_avg"]
        query[0]["cpu_median_min"][i] = coreMetrics["median_min"]
        query[0]["cpu_median_max"][i] = coreMetrics["median_max"]
        query[0]["cpu_min"][i] = coreMetrics["min"]
        query[0]["cpu_max"][i] = coreMetrics["max"]
        query[0]["cpu_q1_min"][i] = coreMetrics["q1_min"]
        query[0]["cpu_q1_max"][i] = coreMetrics["q1_max"]
        query[0]["cpu_q2_min"][i] = coreMetrics["q2_min"]
        query[0]["cpu_q2_max"][i] = coreMetrics["q2_max"]
        query[0]["cpu_q3_min"][i] = coreMetrics["q3_min"]
        query[0]["cpu_q3_max"][i] = coreMetrics["q3_max"]
        query[0]["cpu_p95_min"][i] = coreMetrics["p95_min"]
        query[0]["cpu_p95_max"][i] = coreMetrics["p95_max"]
    
    return query

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)
 
def main():
    # Takes arguments
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    containerID = sys.argv[4]
    hostID = sys.argv[5]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("cpu analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "trial_cpu"
    srcTableCore = "trial_cpu_core"
    destTable = "exp_cpu"
    destTableCores = "exp_cpu_core"
    
    CassandraRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
        .select("cpu_min", "cpu_max", "cpu_q1", "cpu_q2", "cpu_q3", "cpu_p95", "cpu_median", "cpu_num_data_points", "cpu_mean", "cpu_me", "trial_id", "cpu_integral", "cpu_cores") \
        .where("experiment_id=? AND container_id=? AND host_id=?", experimentID, containerID, hostID)
    CassandraRDD.cache()
    
    CassandraRDDFirst = CassandraRDD.first()
    nOfActiveCores = CassandraRDDFirst["cpu_cores"]
    
    query = createQuery(CassandraRDD, experimentID, containerID, hostID, nOfActiveCores)

    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))

    #####################################################
    
    CassandraRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTableCore) \
        .select("cpu_min", "cpu_max", "cpu_q1", "cpu_q2", "cpu_q3", "cpu_p95", "cpu_median", "cpu_num_data_points", "cpu_mean", "cpu_me", "trial_id", "cpu_cores") \
        .where("experiment_id=? AND container_id=? AND host_id=?", experimentID, containerID, hostID)
    CassandraRDD.cache()
    
    query = createCoreQuery(CassandraRDD, experimentID, containerID, hostID, nOfActiveCores)
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTableCores, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()