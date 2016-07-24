import sys
import json

from datetime import timedelta

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def createQuery(sc, cassandraKeyspace, srcTable, experimentID, trialID, containerID, hostID):
    from commons import computeMode, computeMetrics
    
    queries = []
    
    dataRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
            .select("value", "section", "host", "op_name") \
            .where("trial_id=? AND experiment_id=?", trialID, experimentID) \
            .cache() \
            
    hosts = dataRDD.map(lambda a: a["host"]).distinct().collect()
    hosts.append("aggregate")
    operations = dataRDD.map(lambda a: a["op_name"]).distinct().collect()
    sections = ["WebDriver Throughput", "WebDriver Response Times"]
    
    for host in hosts:
        for section in sections:
            for operation in operations:
                modeRDD = dataRDD.filter(lambda r: r["value"] is not None and r["host"] == host and r["op_name"] == operation and section in r["section"]) \
                                 .map(lambda r: (r['value'], 1)) \
                
                mode = computeMode(modeRDD)
                
                data = dataRDD.map(lambda x: x['value']).collect()
                 
                metrics = computeMetrics(data)
                
                queries.append({"experiment_id":experimentID, "trial_id":trialID, "faban_details_host":host, "faban_details_op_name":operation, "faban_details_section":section, \
                          "faban_details_mode":mode[0], "faban_details_mode_freq":mode[1], "faban_details_integral":metrics["integral"], \
                          "faban_details_mean":metrics["mean"], "faban_details_num_data_points":metrics["num_data_points"], \
                          "faban_details_min":metrics["min"], "faban_details_max":metrics["max"], "faban_details_sd":metrics["sd"], "faban_details_variance":metrics["variance"], \
                          "faban_details_q1":metrics["q1"], "faban_details_q2":metrics["q2"], "faban_details_q3":metrics["q3"], "faban_details_p95":metrics["p95"], \
                          "faban_details_p99":metrics["p99"], "faban_details_p90":metrics["p90"], "faban_details_percentiles":metrics["percentiles"], \
                          "faban_details_me":metrics["me"], "faban_details_ci095_min":metrics["ci095_min"], "faban_details_ci095_max":metrics["ci095_max"]})
    return queries

def main():
    #Takes arguments
    args = json.loads(sys.argv[1])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    configFile = str(args["config_file"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Faban trial analyser")
    sc = CassandraSparkContext(conf=conf)

    srcTable = "faban_details"
    destTable = "trial_faban_details"
    
    query = createQuery(sc, cassandraKeyspace, srcTable, experimentID, trialID, containerID, hostID)
    
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, destTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()