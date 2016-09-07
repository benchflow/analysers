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

def createQuery(sc, dataRDD, experimentID, trialID):
    from commons import computeMode, computeMetrics
    
    queries = []
    
    filteredRDD = dataRDD.filter(lambda r: r['duration'] != None).cache()
    
    modeRDD = filteredRDD.map(lambda r: (r['duration'], 1)) \
            .cache()
                   
    mode = computeMode(modeRDD)
    
    data = filteredRDD.map(lambda r: r['duration']).collect()
    metrics = computeMetrics(data)
    
    queries.append({"process_definition_id":"all", "experiment_id":experimentID, "trial_id":trialID, "process_duration_mode":mode[0], "process_duration_mode_freq":mode[1], \
              "process_duration_mean":metrics["mean"], "process_duration_num_data_points":metrics["num_data_points"], \
              "process_duration_min":metrics["min"], "process_duration_max":metrics["max"], "process_duration_sd":metrics["sd"], "process_duration_variance":metrics["variance"], \
              "process_duration_q1":metrics["q1"], "process_duration_q2":metrics["q2"], "process_duration_q3":metrics["q3"], "process_duration_p95":metrics["p95"], \
              "process_duration_me":metrics["me"], "process_duration_ci095_min":metrics["ci095_min"], "process_duration_ci095_max":metrics["ci095_max"], \
              "process_duration_p90":metrics["p90"], "process_duration_p99":metrics["p99"], "process_duration_percentiles":metrics["percentiles"]})
    
    processes = dataRDD.map(lambda a: a["process_name"]).distinct().collect()
    
    for process in processes:
        filteredRDD = dataRDD.filter(lambda r: r['duration'] != None and r['process_name'] == process).cache()
        
        modeRDD = filteredRDD.map(lambda r: (r['duration'], 1)) \
            .cache()
                   
        mode = computeMode(modeRDD)
        
        data = filteredRDD.map(lambda r: r['duration']).collect()
        metrics = computeMetrics(data)
        
        queries.append({"process_definition_id":process, "experiment_id":experimentID, "trial_id":trialID, "process_duration_mode":mode[0], "process_duration_mode_freq":mode[1], \
                  "process_duration_mean":metrics["mean"], "process_duration_num_data_points":metrics["num_data_points"], \
                  "process_duration_min":metrics["min"], "process_duration_max":metrics["max"], "process_duration_sd":metrics["sd"], "process_duration_variance":metrics["variance"], \
                  "process_duration_q1":metrics["q1"], "process_duration_q2":metrics["q2"], "process_duration_q3":metrics["q3"], "process_duration_p95":metrics["p95"], \
                  "process_duration_me":metrics["me"], "process_duration_ci095_min":metrics["ci095_min"], "process_duration_ci095_max":metrics["ci095_max"], \
                  "process_duration_p90":metrics["p90"], "process_duration_p99":metrics["p99"], "process_duration_percentiles":metrics["percentiles"]})
    
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    partitionsPerCore = 5
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Process duration analyser")
    sc = CassandraSparkContext(conf=conf)
    
    srcTable = "process"
    destTable = "trial_process_duration"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
            .select("process_name", "to_ignore", "source_process_instance_id", "start_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["process_name"] is not None and r["to_ignore"] is False) \
            .repartition(sc.defaultParallelism * partitionsPerCore) \
            .cache()
    
    query = createQuery(sc, dataRDD, experimentID, trialID)
    
    sc.parallelize(query, sc.defaultParallelism * partitionsPerCore).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__': main()