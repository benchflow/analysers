import sys
import json

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def createQuery(dataRDD, experimentID):
    from commons import computeMode, computeMetrics
    
    queries = []
    
    processes = dataRDD.map(lambda a: a["process_definition_id"]).distinct().collect()
    
    for process in processes:
        filteredRDD = dataRDD.filter(lambda a: a["process_definition_id"] == process).cache()
        
        mode = computeMode(filteredRDD.map(lambda r: (r['execution_time'], 1)))
    
        data = filteredRDD.map(lambda r: r['execution_time']).collect()
         
        metrics = computeMetrics(data)
        
        queries.append({"process_definition_id": process, "experiment_id":experimentID, "execution_time_mode":mode[0], "execution_time_mode_freq":mode[1], \
                  "execution_time_mean":metrics["mean"], "execution_time_num_data_points":metrics["num_data_points"], \
                  "execution_time_min":metrics["min"], "execution_time_max":metrics["max"], "execution_time_sd":metrics["sd"], "execution_time_variance":metrics["variance"], \
                  "execution_time_q1":metrics["q1"], "execution_time_q2":metrics["q2"], "execution_time_q3":metrics["q3"], "execution_time_p95":metrics["p95"], \
                  "execution_time_p90":metrics["p90"], "execution_time_p99":metrics["p99"], "execution_time_percentiles":metrics["percentiles"], \
                  "execution_time_me":metrics["me"], "execution_time_ci095_min":metrics["ci095_min"], "execution_time_ci095_max":metrics["ci095_max"]})
        
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Execution time analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "trial_execution_time"
    destTable = "exp_execution_time"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("execution_time", "process_definition_id") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['execution_time'] is not None) \
            .cache()
            
    query = createQuery(dataRDD, experimentID)
    
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__':
    main()