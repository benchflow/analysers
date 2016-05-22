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

def createQuery(op, dev, sc, cassandraKeyspace, srcTable, experimentID, trialID, containerID):
    from commons import computeMode, computeMetrics
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("device", op) \
            .where("experiment_id=? AND container_id=?", experimentID, containerID) \
            .filter(lambda a: a["device"] == dev and a[op] is not None) \
            .map(lambda a: (a[op], 1)) \
            .cache()
    
    if len(dataRDD.collect()) == 0:
        return {"experiment_id":experimentID, "container_id":containerID, "device":dev}
        
    mode = computeMode(dataRDD)
    
    data = dataRDD.map(lambda x: x[0]) \
            .collect()
     
    metrics = computeMetrics(data)

    # TODO: Fix this
    query = {"experiment_id":experimentID, "container_id":containerID, "device":dev, op+"_mode":mode[0], \
              op+"_mode_freq":mode[1], op+"_median":metrics["median"], op+"_avg":metrics["mean"], \
              op+"_min":metrics["min"], op+"_max":metrics["max"], op+"_sd":metrics["sd"], \
              op+"_q1":metrics["q1"], op+"_q2":metrics["q2"], op+"_q3":metrics["q3"], op+"_p95":metrics["p95"], \
              op+"_me":metrics["me"], op+"_ci095_min":metrics["ci095_min"], op+"_ci095_max":metrics["ci095_max"]}
    
    return query

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():        
    # Takes arguments
    trialID = sys.argv[1]
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    containerID = sys.argv[4]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("IO analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "trial_io"
    destTable = "exp_io"
    
    data = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable)\
            .select("device") \
            .where("experiment_id=? AND container_id=?", experimentID, containerID) \
            .collect()
    
    devices = {}
    for e in data:
        dev = e["device"]
        devices[dev] = 0
        
    queries = []
    
    for k in devices.keys():
        query = {}
        query.update(createQuery("reads", k, sc, analyserConf["cassandra_keyspace"], srcTable, experimentID, trialID, containerID))
        query.update(createQuery("writes", k, sc, analyserConf["cassandra_keyspace"], srcTable, experimentID, trialID, containerID))
        query.update(createQuery("total", k, sc, analyserConf["cassandra_keyspace"], srcTable, experimentID, trialID, containerID))
        queries.append(query)
    
    sc.parallelize(queries).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()