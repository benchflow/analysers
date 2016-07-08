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

def byteSizeFromString(arg):
    arg = arg.replace(" ", "")
    mUnit = ""
    if not arg[-1].isdigit() and not arg[-2].isdigit():
        mUnit = arg[-2:]
        mUnit = mUnit.lower()
    elif not arg[-1].isdigit() and arg[-2].isdigit():
        mUnit = arg[-1]
        mUnit = mUnit.lower()
    elif arg.isdigit():
        return long(arg)
    else:
        return None
    if "kb" in mUnit or "k" in mUnit:
        return long(arg[:-2]) *1024
    elif "mb" in mUnit or "m" in mUnit:
        return long(arg[:-2]) *1024 *1024
    elif "gb" in mUnit or "g" in mUnit:
        return long(arg[:-2]) *1024 *1024 *1024
    elif "tb" in mUnit or "t" in mUnit:
        return long(arg[:-2]) *1024 *1024 *1024 *1024
    else:
        return long(arg[:-2])
        

def absoluteRamEfficency(sc, cassandraKeyspace, trialID, experimentID, containerID, hostID, dataIntegral, dataPoints):
    # Taking from the container properties
    containerProperties = sc.cassandraTable(cassandraKeyspace, "container_properties") \
            .select("mem_limit") \
            .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
            .first()
    maxMemory = containerProperties["mem_limit"]
    # If Docker returns memory as either 0 or -1 it means unlimited memory given to the container, so we need the host memory
    if maxMemory is not None:
        byteSizeFromString(maxMemory)
    if maxMemory is None or maxMemory < 1:
        hostProperties = sc.cassandraTable(cassandraKeyspace, "host_properties") \
            .select("mem_total") \
            .where("host_id=?", hostID) \
            .first()
        maxMemory = hostProperties["mem_total"]
        if maxMemory is None:
            return None
    absoluteEfficency = dataIntegral/float(long(maxMemory)*dataPoints)
    return absoluteEfficency

def createQuery(dataRDD, sc, cassandraKeyspace, experimentID, trialID, containerID, hostID):
    from commons import computeMode, computeMetrics
    
    mode = computeMode(dataRDD)
    
    data = dataRDD.map(lambda x: x[0]).collect()
     
    metrics = computeMetrics(data)
    relativeEfficency = metrics["integral"]/(metrics["max"]*metrics["num_data_points"])
    absoluteEfficency = absoluteRamEfficency(sc, cassandraKeyspace, trialID, experimentID, containerID, hostID, metrics["integral"], metrics["num_data_points"])
    
    query = [{"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "host_id":hostID, \
              "ram_mode":mode[0], "ram_mode_freq":mode[1], "ram_integral":metrics["integral"], \
              "relative_efficency":relativeEfficency, "absolute_efficency":absoluteEfficency, \
              "ram_mean":metrics["mean"], "ram_num_data_points":metrics["num_data_points"], \
              "ram_min":metrics["min"], "ram_max":metrics["max"], "ram_sd":metrics["sd"], \
              "ram_q1":metrics["q1"], "ram_q2":metrics["q2"], "ram_q3":metrics["q3"], "ram_p95":metrics["p95"], \
              "ram_me":metrics["me"], "ram_ci095_min":metrics["ci095_min"], "ram_ci095_max":metrics["ci095_max"], \
              "ram_p90":metrics["p90"], "ram_p99":metrics["p99"], "ram_percentiles":metrics["percentiles"]}]
    return query

def main():
    #Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Ram trial analyser")
    sc = CassandraSparkContext(conf=conf)
    
    srcTable = "environment_data"
    destTable = "trial_ram"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("memory_usage") \
            .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
            .filter(lambda r: r["memory_usage"] is not None) \
            .map(lambda r: (r['memory_usage'], 1)) \
            .cache()
    
    query = createQuery(dataRDD, sc, cassandraKeyspace, experimentID, trialID, containerID, hostID)
    
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()