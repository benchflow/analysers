from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

def testEmpty(sc):
    from cpu import computeExperimentCoreMetrics
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentCoreMetrics(dataRDD, 0)
    assert result["median_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["median_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["min"] is None, "Experiment metric value incorrect, expected None"
    assert result["max"] is None, "Experiment metric value incorrect, expected None"
    assert result["q1_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q1_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["q2_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q2_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["p95_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["p95_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q3_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q3_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["weighted_avg"] is None, "Experiment metric value incorrect, expected None"
    
def testOneElement(sc):
    from cpu import computeExperimentCoreMetrics
    
    data = [{"cpu_median":[1, 1], "cpu_mean":[1, 1], "cpu_min":[1, 1], "cpu_max":[1, 1], "cpu_q1":[1, 1], \
             "cpu_q2":[1, 1], "cpu_q3":[1, 1], "cpu_p95":[1, 1], "cpu_num_data_points":1, \
             "trial_id":"foo_1", "experiment_id":"foo"}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentCoreMetrics(dataRDD, 0)
    assert result["median_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["median_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q1_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q1_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q2_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q2_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["p95_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["p95_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["weighted_avg"] == 1, "Experiment metric value incorrect, expected 1"
    
def testTwoElements(sc):
    from cpu import computeExperimentCoreMetrics
    
    data = [{"cpu_median":[1, 1], "cpu_mean":[1, 1], "cpu_min":[1, 1], "cpu_max":[1, 1], "cpu_q1":[1, 1], \
             "cpu_q2":[1, 1], "cpu_q3":[1, 1], "cpu_p95":[1, 1], "cpu_num_data_points":1, \
             "trial_id":"foo_1", "experiment_id":"foo"}, \
            {"cpu_median":[2, 2], "cpu_mean":[2, 2], "cpu_min":[2, 2], "cpu_max":[2, 2], "cpu_q1":[2, 2], \
             "cpu_q2":[2, 2], "cpu_q3":[2, 2], "cpu_p95":[2, 2], "cpu_num_data_points":1, \
             "trial_id":"foo_2", "experiment_id":"foo"}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentCoreMetrics(dataRDD, 0)
    assert result["median_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["median_max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["q1_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q1_max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["q2_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q2_max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["p95_max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["p95_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["weighted_avg"] == 1.5, "Experiment metric value incorrect, expected 1.5"
    
def testNullElements(sc):
    from cpu import computeExperimentCoreMetrics
    
    data = [{"cpu_median":[None, 1], "cpu_mean":[None, 1], "cpu_min":[None, 1], "cpu_max":[None, 1], "cpu_q1":[None, 1], \
             "cpu_q2":[None, 1], "cpu_q3":[None, 1], "cpu_p95":[None, 1], "cpu_num_data_points":1, \
             "trial_id":"foo_1", "experiment_id":"foo"}, \
            {"cpu_median":[None, 1], "cpu_mean":[None, 1], "cpu_min":[None, 1], "cpu_max":[None, 1], "cpu_q1":[None, 1], \
             "cpu_q2":[None, 1], "cpu_q3":[None, 1], "cpu_p95":[None, 1], "cpu_num_data_points":1, \
             "trial_id":"foo_2", "experiment_id":"foo"}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentCoreMetrics(dataRDD, 0)
    assert result["median_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["median_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["min"] is None, "Experiment metric value incorrect, expected None"
    assert result["max"] is None, "Experiment metric value incorrect, expected None"
    assert result["q1_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q1_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["q2_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q2_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["p95_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["p95_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q3_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["q3_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["weighted_avg"] is None, "Experiment metric value incorrect, expected None"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testTwoElements(sc)
    testNullElements(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()