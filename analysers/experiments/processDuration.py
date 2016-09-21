import sys
import json
import io
import gzip
import uuid
import math

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

#Create the queries containg the results of the computations to pass to Cassandra
def createQuery(CassandraRDD, experimentID):
    from commons import computeExperimentMetrics, computeModeMinMax, computeCombinedVar
    
    queries = []
    
    processes = CassandraRDD.map(lambda a: a["process_definition_id"]).distinct().collect()
    
    #Iterating over all process definitions
    for process in processes:
        dataRDD = CassandraRDD.filter(lambda a: a["process_definition_id"] == process)
        metrics = computeExperimentMetrics(dataRDD, "process_duration")
        metrics.update(computeModeMinMax(dataRDD, "process_duration"))
        
        combinedVar = computeCombinedVar(dataRDD, "process_duration")
        
        queries.append({"process_definition_id":process, "experiment_id":experimentID, "process_duration_mode_min":metrics["min"], "process_duration_mode_max":metrics["max"], \
                  "process_duration_mode_min_freq":metrics["mode_min_freq"], "process_duration_mode_max_freq":metrics["mode_max_freq"], \
                  "process_duration_mean_min":metrics["mean_min"], "process_duration_mean_max":metrics["mean_max"], \
                  "process_duration_min":metrics["min"], "process_duration_max":metrics["max"], "process_duration_q1_min":metrics["q1_min"], \
                  "process_duration_q1_max":metrics["q1_max"], "process_duration_q2_min":metrics["q2_min"], "process_duration_q2_max":metrics["q2_max"], \
                  "process_duration_p90_max":metrics["p90_max"], "process_duration_p90_min":metrics["p90_min"], \
                  "process_duration_p95_max":metrics["p95_max"], "process_duration_p95_min":metrics["p95_min"], \
                  "process_duration_p99_max":metrics["p99_max"], "process_duration_p99_min":metrics["p99_min"], \
                  "process_duration_q3_min":metrics["q3_min"], "process_duration_q3_max":metrics["q3_max"], "process_duration_weighted_avg":metrics["weighted_avg"], \
                  "process_duration_best": metrics["best"], "process_duration_worst": metrics["worst"], "process_duration_average": metrics["average"], \
                  "process_duration_variation_coefficient": metrics["variation_coefficient"], "process_duration_combined_variance": combinedVar})
    
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Process duration analyser")
    sc = CassandraSparkContext(conf=conf)

    #Source and destination tables
    srcTable = "trial_process_duration"
    destTable = "exp_process_duration"
    
    #Retrieving data for computations
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("process_duration_min", "process_duration_max", "process_duration_q1", "process_duration_q2", "process_duration_q3", \
                "process_duration_p95", "process_duration_num_data_points", "process_duration_mean", \
                "process_duration_me", "trial_id", "process_duration_mode", "process_duration_mode_freq", "process_definition_id", \
                "process_duration_p90", "process_duration_p99", "process_duration_variance") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    #Creating Cassandra query
    query = createQuery(CassandraRDD, experimentID)

    #Save to Cassandra
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__':
    main()