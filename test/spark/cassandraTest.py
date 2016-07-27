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

def main():
    cassandraKeyspace = "benchflow"
    trialID = "camundaZZZZZZMV_ZZZZZZOX"
    experimentID = "camundaZZZZZZMV"
    containerID = "stats_camunda"
    containerName = "stats_camunda"
    hostID = "docker_host"
    
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local[*]") \
        .set("spark.cassandra.connection.host", "cassandra")
    sc = CassandraSparkContext(conf=conf)
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_io")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=? AND container_name=? AND host_id=?", trialID, experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_byte_size")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_number_of_process_instances")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .count()
            
    assert data is not None and data == 3, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_throughput")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .count()
            
    assert data is not None and data == 3, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_ram")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=? AND container_name=? AND host_id=?", trialID, experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_cpu_core")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=? AND container_name=? AND host_id=?", trialID, experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_cpu")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=? AND container_name=? AND host_id=?", trialID, experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "trial_process_duration")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .count()
            
    assert data is not None and data == 3, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_io")\
            .select("experiment_id") \
            .where("experiment_id=? AND container_name=? AND host_id=?", experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_byte_size")\
            .select("experiment_id") \
            .where("experiment_id=?", experimentID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_number_of_process_instances")\
            .select("experiment_id") \
            .where("experiment_id=?", experimentID) \
            .count()
            
    assert data is not None and data == 3, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_throughput")\
            .select("experiment_id") \
            .where("experiment_id=?", experimentID) \
            .count()
            
    assert data is not None and data == 3, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_ram")\
            .select("experiment_id") \
            .where("experiment_id=? AND container_name=? AND host_id=?", experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_cpu_core")\
            .select("experiment_id") \
            .where("experiment_id=? AND container_name=? AND host_id=?", experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_cpu")\
            .select("experiment_id") \
            .where("experiment_id=? AND container_name=? AND host_id=?", experimentID, containerName, hostID) \
            .count()
            
    assert data is not None and data == 1, "Test failed, missing data on Cassandra"
    
    data = sc.cassandraTable(cassandraKeyspace, "exp_process_duration")\
            .select("experiment_id") \
            .where("experiment_id=?", experimentID) \
            .count()
            
    assert data is not None and data == 3, "Test failed, missing data on Cassandra"
    
    print("Cassandra tests passed")
    
if __name__ == '__main__': main()