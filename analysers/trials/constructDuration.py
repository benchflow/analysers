import sys
import json

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

#Create the queries containg the results of the computations to pass to Cassandra
def createQuery(sc, dataRDD, experimentID, trialID):
    from commons import computeMode, computeMetrics
    
    queries = []
    
    filteredRDD = dataRDD.filter(lambda r: r['duration'] != None).cache()
    
    modeRDD = filteredRDD.map(lambda r: (r['duration'], 1)) \
            .cache()
                   
    mode = computeMode(modeRDD)
    
    data = filteredRDD.map(lambda r: r['duration']).collect()
    metrics = computeMetrics(data)
    
    queries.append({"construct_name":"all", "construct_type":"all", "experiment_id":experimentID, "trial_id":trialID, "construct_duration_mode":mode[0], \
                    "construct_duration_mode_freq":mode[1], "construct_duration_p90":metrics["p90"], "construct_duration_p99":metrics["p99"], \
                    "construct_duration_percentiles":metrics["percentiles"], \
                  "construct_duration_mean":metrics["mean"], "construct_duration_num_data_points":metrics["num_data_points"], \
                  "construct_duration_min":metrics["min"], "construct_duration_max":metrics["max"], "construct_duration_sd":metrics["sd"], "construct_duration_variance":metrics["variance"], \
                  "construct_duration_q1":metrics["q1"], "construct_duration_q2":metrics["q2"], "construct_duration_q3":metrics["q3"], "construct_duration_p95":metrics["p95"], \
                  "construct_duration_me":metrics["me"], "construct_duration_ci095_min":metrics["ci095_min"], "construct_duration_ci095_max":metrics["ci095_max"]})
    
    combinations = dataRDD.map(lambda a: (a["construct_type"], a["construct_name"])).distinct().collect()
    
    #Iterate over all combinations of construct name and type
    for combs in combinations:
        consType = combs[0]
        name = combs[1]
        
        filteredRDD = dataRDD.filter(lambda r: r['duration'] != None and r['construct_name'] == name and r['construct_type'] == consType)
        
        modeRDD = filteredRDD.map(lambda r: (r['duration'], 1)) \
            .cache()
                   
        mode = computeMode(modeRDD)
        
        data = filteredRDD.map(lambda r: r['duration']).collect()
        metrics = computeMetrics(data)
        
        # Checking for type and name being None, in order to avoid saving a None type to the trials table
        if consType is None:
            consType = "Unspecified"
        if name is None:
            name = "Unspecified"
        
        queries.append({"construct_name":name, "construct_type":consType, "experiment_id":experimentID, "trial_id":trialID, "construct_duration_mode":mode[0], \
                    "construct_duration_mode_freq":mode[1], "construct_duration_p90":metrics["p90"], "construct_duration_p99":metrics["p99"], \
                    "construct_duration_percentiles":metrics["percentiles"], \
                  "construct_duration_mean":metrics["mean"], "construct_duration_num_data_points":metrics["num_data_points"], \
                  "construct_duration_min":metrics["min"], "construct_duration_max":metrics["max"], "construct_duration_sd":metrics["sd"], "construct_duration_variance":metrics["variance"], \
                  "construct_duration_q1":metrics["q1"], "construct_duration_q2":metrics["q2"], "construct_duration_q3":metrics["q3"], "construct_duration_p95":metrics["p95"], \
                  "construct_duration_me":metrics["me"], "construct_duration_ci095_min":metrics["ci095_min"], "construct_duration_ci095_max":metrics["ci095_max"]})

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
    conf = SparkConf().setAppName("Construct duration analyser")
    sc = CassandraSparkContext(conf=conf)

    #Source and destination tables
    srcTable = "construct"
    destTable = "trial_construct_duration"
    
    #Retrieving data for computations
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
            .select("source_construct_instance_id", "to_ignore", "construct_name", "construct_type", "start_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["source_construct_instance_id"] is not None and r["to_ignore"] is False) \
            .repartition(sc.defaultParallelism * partitionsPerCore) \
            .cache()
    
    #Create Cassandra table
    query = createQuery(sc, dataRDD, experimentID, trialID)
    
    #Save to Cassandra
    sc.parallelize(query, sc.defaultParallelism * partitionsPerCore).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__': main()