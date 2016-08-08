import sys
import json

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def createQuery(dataRDD, experimentID):
    from commons import computeMode, computeMetrics
    
    queries = []
    
    combinations = dataRDD.map(lambda a: (a["construct_type"], a["construct_name"])).distinct().collect()
    
    for combs in combinations:
        consType = combs[0]
        name = combs[1]
        
        mode = computeMode(dataRDD.filter(lambda r: r['construct_name'] == name and r['construct_type'] == consType).map(lambda r: (r['number_of_construct_instances'], 1)))
    
        data = dataRDD.filter(lambda r: r['construct_name'] == name and r['construct_type'] == consType).map(lambda r: r['number_of_construct_instances']).collect()
         
        metrics = computeMetrics(data)
        
        if consType is None:
            consType = "Unspecified"
        if name is None:
            name = "Unspecified"
        
        queries.append({"experiment_id":experimentID, "number_of_construct_instances_mode":mode[0], "number_of_construct_instances_mode_freq":mode[1], \
                 "construct_type": consType, "construct_name": name, \
                  "number_of_construct_instances_mean":metrics["mean"], "number_of_construct_instances_num_data_points":metrics["num_data_points"], \
                  "number_of_construct_instances_min":metrics["min"], "number_of_construct_instances_max":metrics["max"], "number_of_construct_instances_sd":metrics["sd"], "number_of_construct_instances_variance":metrics["variance"], \
                  "number_of_construct_instances_q1":metrics["q1"], "number_of_construct_instances_q2":metrics["q2"], "number_of_construct_instances_q3":metrics["q3"], "number_of_construct_instances_p95":metrics["p95"], \
                  "number_of_construct_instances_p90":metrics["p90"], "number_of_construct_instances_p99":metrics["p99"], "number_of_construct_instances_percentiles":metrics["percentiles"], \
                  "number_of_construct_instances_me":metrics["me"], "number_of_construct_instances_ci095_min":metrics["ci095_min"], "number_of_construct_instances_ci095_max":metrics["ci095_max"]})
        
    return queries

def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of construct instances analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "trial_number_of_construct_instances"
    destTable = "exp_number_of_construct_instances"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("number_of_construct_instances", "construct_name", "construct_type") \
            .where("experiment_id=?", experimentID) \
            .filter(lambda r: r['number_of_construct_instances'] is not None) \
            .cache()
            
    query = createQuery(dataRDD, experimentID)
    
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__':
    main()