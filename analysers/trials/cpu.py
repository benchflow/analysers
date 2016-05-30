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

def getActiveCores(sc, cassandraKeyspace, srcTable, trialID, experimentID, containerID, hostID):
    cpuInfoAvailable = True
    
    try:
        containerProperties = sc.cassandraTable(cassandraKeyspace, "container_properties") \
                .select("cpu_set_cpus") \
                .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
                .first()
    except:
        cpuInfoAvailable = False
            
    if cpuInfoAvailable and "cpu_set_cpus" in containerProperties.keys():
        cpuSet = containerProperties["cpu_set_cpus"].split(",")
        nOfCpus = 0
        for c in cpuSet:
            cpuRange = c.split("-")
            if len(cpuRange) == 2:
                nOfCpus += (len(range(cpuRange[0], cpuRange[1]))+1)
            else:
                nOfCpus += 1
        return nOfCpus
    
    else:
        nOfActiveCores = sc.cassandraTable(cassandraKeyspace, srcTable) \
                .select("cpu_percpu_usage") \
                .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
                .first()
                
        nOfCpus = len(nOfActiveCores["cpu_percpu_usage"])
        return nOfCpus

def createQuery(dataRDD, experimentID, trialID, containerID, hostID, nOfActiveCores):
    from commons import computeMetrics
    
    metrics = computeMetrics(dataRDD)
    
    query = [{"experiment_id":experimentID, "trial_id":trialID, "container_id":containerID, "host_id":hostID, "cpu_median":metrics["median"], \
              "cpu_mean":metrics["mean"], "cpu_num_data_points":metrics["num_data_points"], \
              "cpu_min":metrics["min"], "cpu_max":metrics["max"], "cpu_sd":metrics["sd"], \
              "cpu_q1":metrics["q1"], "cpu_q2":metrics["q2"], "cpu_q3":metrics["q3"], "cpu_p95":metrics["p95"], \
              "cpu_me":metrics["me"], "cpu_ci095_min":metrics["ci095_min"], "cpu_ci095_max":metrics["ci095_max"], \
              "cpu_integral":metrics["integral"], "cpu_cores":nOfActiveCores}]
    
    return query

def createCoresQuery(dataRDD, experimentID, trialID, containerID, hostID, nOfActiveCores):
    from commons import computeMetrics
    
    nOfCores = len(dataRDD.first())
    
    query = [{}]
    query[0]["experiment_id"] = experimentID
    query[0]["trial_id"] = trialID
    query[0]["container_id"] = containerID
    query[0]["host_id"] = hostID
    query[0]["cpu_cores"] = nOfActiveCores
    query[0]["cpu_num_data_points"] = None
    query[0]["cpu_median"] = [None]*nOfCores
    query[0]["cpu_mean"] = [None]*nOfCores
    query[0]["cpu_integral"] = [None]*nOfCores
    query[0]["cpu_min"] = [None]*nOfCores
    query[0]["cpu_max"] = [None]*nOfCores
    query[0]["cpu_sd"] = [None]*nOfCores
    query[0]["cpu_q1"] = [None]*nOfCores
    query[0]["cpu_q2"] = [None]*nOfCores
    query[0]["cpu_q3"] = [None]*nOfCores
    query[0]["cpu_p95"] = [None]*nOfCores
    query[0]["cpu_me"] = [None]*nOfCores
    query[0]["cpu_ci095_min"] = [None]*nOfCores
    query[0]["cpu_ci095_max"] = [None]*nOfCores
    
    for i in range(nOfCores):
        data = dataRDD.map(lambda r: r[i]) \
                .collect()
    
        met = computeMetrics(data)
        query[0]["cpu_num_data_points"] = met["num_data_points"]
        query[0]["cpu_median"][i] = met["median"]
        query[0]["cpu_mean"][i] = met["mean"]
        query[0]["cpu_integral"][i] = met["integral"]
        query[0]["cpu_min"][i] = met["min"]
        query[0]["cpu_max"][i] = met["max"]
        query[0]["cpu_sd"][i] = met["sd"]
        query[0]["cpu_q1"][i] = met["q1"]
        query[0]["cpu_q2"][i] = met["q2"]
        query[0]["cpu_q3"][i] = met["q3"]
        query[0]["cpu_p95"][i] = met["p95"]
        query[0]["cpu_me"][i] = met["me"]
        query[0]["cpu_ci095_min"][i] = met["ci095_min"]
        query[0]["cpu_ci095_max"][i] = met["ci095_max"]
        
    return query

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Cpu analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "environment_data"
    destTable = "trial_cpu"
    
    nOfActiveCores = getActiveCores(sc, analyserConf["cassandra_keyspace"], srcTable, trialID, experimentID, containerID, hostID)
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("cpu_percent_usage") \
            .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
            .filter(lambda r: r['cpu_percent_usage'] is not None) \
            .map(lambda r: r['cpu_percent_usage']) \
            .collect()
    
    query = createQuery(dataRDD, experimentID, trialID, containerID, hostID, nOfActiveCores)
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], "trial_cpu")
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("cpu_percpu_percent_usage") \
            .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trialID, experimentID, containerID, hostID) \
            .filter(lambda r: r['cpu_percpu_percent_usage'] is not None) \
            .map(lambda r: r['cpu_percpu_percent_usage']) \
            .cache()
    
    query = createCoresQuery(dataRDD, experimentID, trialID, containerID, hostID, nOfActiveCores)
       
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], "trial_cpu_core", ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()