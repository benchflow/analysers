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

def createQuery(op, dev, sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    from commons import computeMode, computeMetrics
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("device", op) \
            .where("experiment_id=? AND container_id=? AND host_id=?", experimentID, containerID, hostID) \
            .filter(lambda a: a["device"] == dev and a[op] is not None) \
            .map(lambda a: (a[op], 1)) \
            .cache()
    
    if len(dataRDD.collect()) == 0:
        return {"experiment_id":experimentID, "container_id":containerID, "host_id":hostID, "device":dev}
        
    mode = computeMode(dataRDD)
    
    data = dataRDD.map(lambda x: x[0]) \
            .collect()
     
    metrics = computeMetrics(data)

    # TODO: Fix this
    query = {"experiment_id":experimentID, "container_id":containerID, "host_id":hostID, "device":dev, op+"_mode":mode[0], \
              op+"_mode_freq":mode[1], op+"_mean":metrics["mean"], \
              op+"_min":metrics["min"], op+"_max":metrics["max"], op+"_sd":metrics["sd"], \
              op+"_q1":metrics["q1"], op+"_q2":metrics["q2"], op+"_q3":metrics["q3"], op+"_p95":metrics["p95"], \
              op+"_p90":metrics["p90"], op+"_p99":metrics["p99"], op+"_percentiles":metrics["percentiles"], \
              op+"_me":metrics["me"], op+"_ci095_min":metrics["ci095_min"], op+"_ci095_max":metrics["ci095_max"]}
    
    return query

def main():        
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("IO analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "trial_io"
    destTable = "exp_io"
    
    data = sc.cassandraTable(cassandraKeyspace, srcTable)\
            .select("device") \
            .where("experiment_id=? AND container_id=? AND host_id=?", experimentID, containerID, hostID) \
            .collect()
    
    devices = {}
    for e in data:
        dev = e["device"]
        devices[dev] = 0
        
    queries = []
    
    for k in devices.keys():
        query = {}
        query.update(createQuery("reads", k, sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID))
        query.update(createQuery("writes", k, sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID))
        query.update(createQuery("total", k, sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID))
        queries.append(query)
    
    sc.parallelize(queries).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()