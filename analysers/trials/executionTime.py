import sys
import json

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def computeExecutionTime(dataRDD):
    if(dataRDD.isEmpty()):
        return None
    
    data = dataRDD.map(lambda r: (r['start_time'], r['end_time'])) \
            .collect()
    
    # Finding largest and smallest times, computing delta seconds
    smallest = data[0][0]
    largest = data[0][1]
    for d in data[1:]:
        tS = d[0]
        tL = d[1]
        if tS < smallest:
            smallest = tS
        if tL > largest:
            largest = tL
    
    delta = largest - smallest
    delta = delta.total_seconds()
    
    return delta

def createQuery(sc, dataRDD, experimentID, trialID):
    queries = []
    
    tp = computeExecutionTime(dataRDD)
    
    queries.append({"experiment_id":experimentID, "trial_id":trialID, "process_definition_id":"all", "execution_time":tp})
    
    processes = dataRDD.map(lambda a: a["process_name"]).distinct().collect()
    
    for process in processes:
        tp = computeExecutionTime(dataRDD.filter(lambda r: r['process_name'] == process))
    
        queries.append({"experiment_id":experimentID, "trial_id":trialID, "process_definition_id":process, "execution_time":tp})
        
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
    conf = SparkConf().setAppName("Process execution time trial analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "process"
    destTable = "trial_execution_time"
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable)\
            .select("process_name", "to_ignore", "source_process_instance_id", "start_time", "end_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["process_name"] is not None and r["to_ignore"] is False) \
            .repartition(sc.defaultParallelism * partitionsPerCore) \
            .cache()
            
    query = createQuery(sc, dataRDD, experimentID, trialID)
    
    sc.parallelize(query, sc.defaultParallelism * partitionsPerCore).saveToCassandra(cassandraKeyspace, destTable)
    
if __name__ == '__main__': main()