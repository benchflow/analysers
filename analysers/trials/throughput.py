import sys
import json
import io
import gzip
import uuid
import math
import datetime 

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf
from pyspark import SparkFiles

def computeThroughput(dataRDD):
    if(dataRDD.isEmpty()):
        return (None, None)
    
    data = dataRDD.map(lambda r: (r['start_time'], r['end_time'])) \
            .collect()
    
    smallest = None
    for d in data:
        t = d[0]
        if smallest == None:
            smallest = t
        elif t != None:
            if t < smallest:
                smallest = t
            
    largest = None
    for d in data:
        t = d[1]
        if largest == None:
            largest = t
        elif t != None:
            if t > largest:
                largest = t
    
    if largest is None or smallest is None:
        return (None, None)
    
    delta = largest - smallest
    delta = delta.total_seconds()
    
    tp = len(data)/float(delta)
    
    return (tp, delta)

def createQuery(sc, dataRDD, experimentID, trialID, nToIgnore):
    from commons import cutNInitialProcesses
    
    data = cutNInitialProcesses(dataRDD, nToIgnore)
        
    dataRDD = sc.parallelize(data)
    
    tp = computeThroughput(dataRDD)
    
    return [{"experiment_id":experimentID, "trial_id":trialID, "throughput":tp[0], "execution_time":tp[1]}]

def getAnalyserConf(SUTName):
    from commons import getAnalyserConfiguration
    return getAnalyserConfiguration(SUTName)

def main():
    # Takes arguments
    trialID = sys.argv[1]
    experimentID = sys.argv[2]
    SUTName = sys.argv[3]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Process duration trial analyser")
    sc = CassandraSparkContext(conf=conf)
    
    analyserConf = getAnalyserConf(SUTName)
    srcTable = "process"
    destTable = "trial_throughput"
    
    dataRDD = sc.cassandraTable(analyserConf["cassandra_keyspace"], srcTable)\
            .select("process_definition_id", "source_process_instance_id", "start_time", "end_time", "duration") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .filter(lambda r: r["process_definition_id"] is not None) \
            .cache()
            
    query = createQuery(sc, dataRDD, experimentID, trialID, analyserConf["initial_processes_cut"])
    
    sc.parallelize(query).saveToCassandra(analyserConf["cassandra_keyspace"], destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__': main()