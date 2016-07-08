import sys
import json

from datetime import timedelta

import numpy as np

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
    
def createTotalOpsQuery(sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    from commons import computeExperimentMetrics, computeModeMinMax, computeMetrics, computeLevene
    
    queries = []
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("experiment_id", "host", "name", "total_ops_value", "total_ops_unit") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    combinations = CassandraRDD.map(lambda a: (a["host"], a["name"])).distinct().collect()
    
    for comb in combinations:
        data = CassandraRDD.filter(lambda r: r['host'] == comb[0] and r['name'] == comb[1]).map(lambda r: r["total_ops_value"]).collect()
        avg = np.mean(data).item()   
        queries.append({"experiment_id": experimentID, "faban_driver_host": comb[0], "faban_driver_name": comb[1], "faban_total_ops_mean": avg})
        
    return queries

def createOpsQuery(sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    from commons import computeMetrics, computeMode
    
    queries = []
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("experiment_id", "host", "driver_name", "op_name", "successes", "failures", "mix") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    combinations = CassandraRDD.map(lambda a: (a["host"], a["driver_name"], a["op_name"])).distinct().collect()
    
    for comb in combinations:
        query = {}
        for dataName in ["successes", "failures", "mix"]:
            data = CassandraRDD.filter(lambda r: r['host'] == comb[0] and r['driver_name'] == comb[1] and r['op_name'] == comb[2])
            
            mode = computeMode(data.map(lambda r: (r[dataName], 1)))
            metrics = computeMetrics(data.map(lambda r: r[dataName]).collect())
            
            query.update({"experiment_id":experimentID, "faban_driver_host": comb[0], "faban_driver_name": comb[1], "faban_op_name": comb[2], \
                      "faban_"+dataName+"_mode":mode[0], "faban_"+dataName+"_mode_freq":mode[1], \
                      "faban_"+dataName+"_mean":metrics["mean"], "faban_"+dataName+"_num_data_points":metrics["num_data_points"], \
                      "faban_"+dataName+"_min":metrics["min"], "faban_"+dataName+"_max":metrics["max"], "faban_"+dataName+"_sd":metrics["sd"], \
                      "faban_"+dataName+"_q1":metrics["q1"], "faban_"+dataName+"_q2":metrics["q2"], "faban_"+dataName+"_q3":metrics["q3"], "faban_"+dataName+"_p95":metrics["p95"], \
                      "faban_"+dataName+"_p90":metrics["p90"], "faban_"+dataName+"_p99":metrics["p99"], "faban_"+dataName+"_percentiles":metrics["percentiles"], \
                      "faban_"+dataName+"_me":metrics["me"], "faban_"+dataName+"_ci095_min":metrics["ci095_min"], "faban_"+dataName+"_ci095_max":metrics["ci095_max"]})
        queries.append(query)
    return queries

def createDelaysQuery(sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    from commons import computeMetrics, computeMode
    
    queries = []
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("experiment_id", "host", "driver_name", "op_name", "actual_avg", "min", "max") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    combinations = CassandraRDD.map(lambda a: (a["host"], a["driver_name"], a["op_name"])).distinct().collect()
    
    for comb in combinations:
        query = {}
        
        data = CassandraRDD.filter(lambda r: r['host'] == comb[0] and r['driver_name'] == comb[1] and r['op_name'] == comb[2])
        
        mean = data.map(lambda r: r["actual_avg"]).mean()
        
        dataMin = data.map(lambda r: r["min"]).min()
        dataMax = data.map(lambda r: r["max"]).max()
        
        query = {"experiment_id":experimentID, "faban_driver_host": comb[0], "faban_driver_name": comb[1], "faban_op_name": comb[2], \
                 "faban_delay_times_weighted_avg":mean, "faban_delay_times_min":dataMin, "faban_delay_times_max":dataMax}
        
        queries.append(query)
    return queries

def runInfoQuery(sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    from commons import computeMetrics, computeMode
    
    queries = []
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("experiment_id", "host", "duration", "metric_unit", "metric_value", "passed") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    hosts = CassandraRDD.map(lambda a: a["host"]).distinct().collect()
    
    for host in hosts:
        query = {}
        for dataName in ["duration", "metric_value"]:
            data = CassandraRDD.filter(lambda r: r['host'] == host).cache()
            
            mode = computeMode(data.map(lambda r: (r[dataName], 1)))
            metrics = computeMetrics(data.map(lambda r: r[dataName]).collect())
            
            query.update({"experiment_id":experimentID, "faban_host": host, "faban_metric_unit": data.first()["metric_unit"], \
                      "faban_"+dataName+"_mode":mode[0], "faban_"+dataName+"_mode_freq":mode[1], \
                      "faban_"+dataName+"_mean":metrics["mean"], "faban_"+dataName+"_num_data_points":metrics["num_data_points"], \
                      "faban_"+dataName+"_min":metrics["min"], "faban_"+dataName+"_max":metrics["max"], "faban_"+dataName+"_sd":metrics["sd"], \
                      "faban_"+dataName+"_q1":metrics["q1"], "faban_"+dataName+"_q2":metrics["q2"], "faban_"+dataName+"_q3":metrics["q3"], "faban_"+dataName+"_p95":metrics["p95"], \
                      "faban_"+dataName+"_p90":metrics["p90"], "faban_"+dataName+"_p99":metrics["p99"], "faban_"+dataName+"_percentiles":metrics["percentiles"], \
                      "faban_"+dataName+"_me":metrics["me"], "faban_"+dataName+"_ci095_min":metrics["ci095_min"], "faban_"+dataName+"_ci095_max":metrics["ci095_max"]})
        query["faban_passed"] = data.map(lambda r: r["passed"]).reduce(lambda a, b: a and b)
        queries.append(query)
    return queries

def createResponseTimesQuery(sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    queries = []
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("experiment_id", "host", "driver_name", "op_name", "stat_name", "stat_value") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    combinations = CassandraRDD.map(lambda a: (a["host"], a["driver_name"], a["op_name"], a["stat_name"])).distinct().collect()
    
    for comb in combinations:
        query = {}
        
        data = CassandraRDD.filter(lambda r: r['host'] == comb[0] and r['driver_name'] == comb[1] and r['op_name'] == comb[2] and r['stat_name'] == comb[3])
        
        statValues = data.map(lambda r: r["stat_value"]).cache()
        
        dataMin = statValues.min()
        dataMax = statValues.max()
        
        query = {"experiment_id":experimentID, "faban_driver_host": comb[0], "faban_driver_name": comb[1], "faban_op_name": comb[2], \
                 "faban_op_stat_name":comb[3], "faban_op_stat_min":dataMin, "faban_op_stat_max":dataMax}
        
        queries.append(query)
    return queries
        

def createCustomStatsQuery(sc, cassandraKeyspace, srcTable, experimentID, containerID, hostID):
    from commons import computeMetrics, computeMode
    
    absQueries = []
    statQueries = []
    
    CassandraRDD = sc.cassandraTable(cassandraKeyspace, srcTable) \
        .select("experiment_id", "host", "driver_name", "stat_name", "description", "target", "result") \
        .where("experiment_id=?", experimentID)
    CassandraRDD.cache()
    
    combinations = CassandraRDD.map(lambda a: (a["host"], a["driver_name"], a["stat_name"], a["description"])).distinct().collect()
    
    for comb in combinations:
        query = {}
        
        data = CassandraRDD.filter(lambda r: r['host'] == comb[0] and r['driver_name'] == comb[1] and r['stat_name'] == comb[2] and r['description'] == comb[3])
        
        target = data.first()["target"]
        
        if target == "absolute":
            mode = computeMode(data.map(lambda r: (r["result"], 1)))
            metrics = computeMetrics(data.map(lambda r: r["result"]).collect())
            
            query.update({"experiment_id":experimentID, "faban_driver_host": comb[0], "faban_driver_name": comb[1], "faban_stat_name": comb[2], \
                      "faban_stat_mode":mode[0], "faban_stat_mode_freq":mode[1], "faban_stat_description":comb[3],\
                      "faban_stat_mean":metrics["mean"], "faban_stat_num_data_points":metrics["num_data_points"], \
                      "faban_stat_min":metrics["min"], "faban_stat_max":metrics["max"], "faban_stat_sd":metrics["sd"], \
                      "faban_stat_q1":metrics["q1"], "faban_stat_q2":metrics["q2"], "faban_stat_q3":metrics["q3"], "faban_stat_p95":metrics["p95"], \
                      "faban_stat_p90":metrics["p90"], "faban_stat_p99":metrics["p99"], "faban_stat_percentiles":metrics["percentiles"], \
                      "faban_stat_me":metrics["me"], "faban_stat_ci095_min":metrics["ci095_min"], "faban_stat_ci095_max":metrics["ci095_max"]})
            absQueries.append(query)
        
        else:
            mappedData = data.map(lambda r: r["result"]).cache()
            dataMin = mappedData.min()
            dataMax = mappedData.max()
            query.update({"experiment_id":experimentID, "faban_driver_host": comb[0], "faban_driver_name": comb[1], "faban_stat_name": comb[2], \
                          "faban_stat_description":comb[3],\
                          "faban_stat_min":dataMin, "faban_stat_max":dataMax})
            statQueries.append(query)
            
    return (absQueries, statQueries)
        
def main():
    # Takes arguments
    args = json.loads(sys.argv[1])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Faban analyser")
    sc = CassandraSparkContext(conf=conf)

    query = createTotalOpsQuery(sc, cassandraKeyspace, "faban_driver_summary", experimentID, containerID, hostID)
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_faban_total_ops", ttl=timedelta(hours=1))
    
    query = createDelaysQuery(sc, cassandraKeyspace, "faban_driver_delay_times", experimentID, containerID, hostID)
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_faban_delay_times", ttl=timedelta(hours=1))
    
    query = createResponseTimesQuery(sc, cassandraKeyspace, "faban_driver_response_times", experimentID, containerID, hostID)
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_faban_ops_response_times", ttl=timedelta(hours=1))
    
    query = runInfoQuery(sc, cassandraKeyspace, "faban_run_info", experimentID, containerID, hostID)
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_faban_run_info", ttl=timedelta(hours=1))
    
    query = createOpsQuery(sc, cassandraKeyspace, "faban_driver_mix", experimentID, containerID, hostID)
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "exp_faban_ops", ttl=timedelta(hours=1))
    
    query = createCustomStatsQuery(sc, cassandraKeyspace, "faban_driver_custom_stats", experimentID, containerID, hostID)
    sc.parallelize(query[0]).saveToCassandra(cassandraKeyspace, "exp_faban_ops_custom_stats_target_absolute", ttl=timedelta(hours=1))
    sc.parallelize(query[1]).saveToCassandra(cassandraKeyspace, "exp_faban_ops_custom_stats_target_statistic", ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()