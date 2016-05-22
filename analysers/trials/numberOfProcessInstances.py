import sys
import json
import io
import gzip
import uuid

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def createQuery(dataRDD, experimentID, trialID, nToIgnore):
    from commons import cutNInitialProcesses
    
    data = cutNInitialProcesses(dataRDD, nToIgnore)
    
    numberOfInstances = len(data)
    
    return [{"experiment_id":experimentID, "trial_id":trialID, "number_of_process_instances":numberOfInstances}]

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    trialID = sys.argv[1]
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Number of process instances analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "process"
    destTable = "trial_number_of_process_instances"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable) \
            .select("process_definition_id", "source_process_instance_id", "start_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["process_definition_id"] is not None) \
            .cache()
        
    query = createQuery(dataRDD, experimentID, trialID, analyserConf["initial_processes_cut"])
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()