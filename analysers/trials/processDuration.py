import sys
import json
import io
import gzip
import uuid
import math

from datetime import timedelta

import numpy as np

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

def createQuery(sc, dataRDD, experimentID, trialID, nToIgnore):
    from commons import cutNInitialProcesses, computeMode, computeMetrics
    
    data = cutNInitialProcesses(dataRDD, nToIgnore)
    dataRDD = sc.parallelize(data)
    
    dataRDD = dataRDD.filter(lambda r: r['duration'] != None) \
            .map(lambda r: (r['duration'], 1)) \
            .cache()
                   
    mode = computeMode(dataRDD)
    
    data = dataRDD.map(lambda r: r[0]).collect()
    metrics = computeMetrics(data)
    
    return [{"experiment_id":experimentID, "trial_id":trialID, "process_duration_mode":mode[0], "process_duration_mode_freq":mode[1], "process_duration_median":metrics["median"], \
              "process_duration_mean":metrics["mean"], "process_duration_num_data_points":metrics["num_data_points"], \
              "process_duration_min":metrics["min"], "process_duration_max":metrics["max"], "process_duration_sd":metrics["sd"], \
              "process_duration_q1":metrics["q1"], "process_duration_q2":metrics["q2"], "process_duration_q3":metrics["q3"], "process_duration_p95":metrics["p95"], \
              "process_duration_me":metrics["me"], "process_duration_ci095_min":metrics["ci095_min"], "process_duration_ci095_max":metrics["ci095_max"]}]

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Process duration analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "process"
    destTable = "trial_process_duration"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable)\
            .select("process_definition_id", "source_process_instance_id", "start_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["process_definition_id"] is not None) \
            .cache()
    
    query = createQuery(sc, dataRDD, experimentID, trialID, analyserConf["initial_processes_cut"])
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()