from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def testEmpty(sc):
    from commons import computeExperimentMetrics, computeModeMinMax
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentMetrics(dataRDD, "data")
    result.update(computeModeMinMax(dataRDD, "data"))
    assert result["mode_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["mode_max"] is None, "Experiment metric value incorrect, expected None"
    assert result["mode_min_freq"] is None, "Experiment metric value incorrect, expected None"
    assert result["mode_max_freq"] is None, "Experiment metric value incorrect, expected None"
    assert result["mean_min"] is None, "Experiment metric value incorrect, expected None"
    assert result["mean_max"] is None, "Experiment metric value incorrect, expected None"
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
    assert result["best"] is None, "Experiment metric value incorrect, expected None"
    assert result["worst"] is None, "Experiment metric value incorrect, expected None"
    assert result["average"] is None, "Experiment metric value incorrect, expected None"
    
def testOneElement(sc):
    from commons import computeExperimentMetrics, computeModeMinMax
    
    data = [{"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_1", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentMetrics(dataRDD, "data")
    result.update(computeModeMinMax(dataRDD, "data"))
    assert result["mode_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_min_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_max"] == 1, "Experiment metric value incorrect, expected 1"
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
    assert result["best"] == ["foo_1"], "Experiment metric value incorrect, expected foo_1"
    assert result["worst"] == ["foo_1"], "Experiment metric value incorrect, expected foo_1"
    assert result["average"] == ["foo_1"], "Experiment metric value incorrect, expected foo_1"
    
def testTwoElements(sc):
    from commons import computeExperimentMetrics, computeModeMinMax
    
    data = [{"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_1", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[2], "data_mode_freq":1, "data_mean":2, "data_min":2, "data_max":2, "data_q1":2, \
             "data_q2":2, "data_q3":2, "data_p95":2, "data_me":0, "data_num_data_points":1, "trial_id":"foo_2", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentMetrics(dataRDD, "data")
    result.update(computeModeMinMax(dataRDD, "data"))
    assert result["mode_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["mode_min_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_max"] == 2, "Experiment metric value incorrect, expected 2"
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
    assert result["best"] == ["foo_1"], "Experiment metric value incorrect, expected foo_1"
    assert result["worst"] == ["foo_2"], "Experiment metric value incorrect, expected foo_2"
    assert result["average"] == ["foo_1", "foo_2"], "Experiment metric value incorrect, expected foo_1 and foo_2"
    
def testThreeElements(sc):
    from commons import computeExperimentMetrics, computeModeMinMax
    
    data = [{"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_1", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[2], "data_mode_freq":1, "data_mean":2, "data_min":2, "data_max":2, "data_q1":2, \
             "data_q2":2, "data_q3":2, "data_p95":2, "data_me":0, "data_num_data_points":1, "trial_id":"foo_2", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[3], "data_mode_freq":1, "data_mean":3, "data_min":3, "data_max":3, "data_q1":3, \
             "data_q2":3, "data_q3":3, "data_p95":3, "data_me":0, "data_num_data_points":1, "trial_id":"foo_3", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentMetrics(dataRDD, "data")
    result.update(computeModeMinMax(dataRDD, "data"))
    assert result["mode_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["mode_min_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["q1_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q1_max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["q2_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q2_max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["p95_max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["p95_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_max"] == 3, "Experiment metric value incorrect, expected 3"
    assert result["weighted_avg"] == 2, "Experiment metric value incorrect, expected 2"
    assert result["best"] == ["foo_1"], "Experiment metric value incorrect, expected foo_1"
    assert result["worst"] == ["foo_3"], "Experiment metric value incorrect, expected foo_3"
    assert result["average"] == ["foo_2"], "Experiment metric value incorrect, expected foo_2"
    
def testFourElements(sc):
    from commons import computeExperimentMetrics, computeModeMinMax
    
    data = [{"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_1", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[2], "data_mode_freq":1, "data_mean":2, "data_min":2, "data_max":2, "data_q1":2, \
             "data_q2":2, "data_q3":2, "data_p95":2, "data_me":0, "data_num_data_points":1, "trial_id":"foo_2", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[3], "data_mode_freq":1, "data_mean":3, "data_min":3, "data_max":3, "data_q1":3, \
             "data_q2":3, "data_q3":3, "data_p95":3, "data_me":0, "data_num_data_points":1, "trial_id":"foo_3", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[4], "data_mode_freq":1, "data_mean":4, "data_min":4, "data_max":4, "data_q1":4, \
             "data_q2":4, "data_q3":4, "data_p95":4, "data_me":0, "data_num_data_points":1, "trial_id":"foo_4", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentMetrics(dataRDD, "data")
    result.update(computeModeMinMax(dataRDD, "data"))
    assert result["mode_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["mode_min_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["q1_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q1_max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["q2_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q2_max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["p95_max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["p95_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["q3_max"] == 4, "Experiment metric value incorrect, expected 4"
    assert result["weighted_avg"] == 2.5, "Experiment metric value incorrect, expected 2.5"
    assert result["best"] == ["foo_1"], "Experiment metric value incorrect, expected foo_1"
    assert result["worst"] == ["foo_4"], "Experiment metric value incorrect, expected foo_4"
    assert result["average"] == ["foo_2", "foo_3"], "Experiment metric value incorrect, expected foo_2, foo_3"
    
def testAllSameElements(sc):
    from commons import computeExperimentMetrics, computeModeMinMax
    
    data = [{"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_1", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_2", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}, \
            {"data_mode":[1], "data_mode_freq":1, "data_mean":1, "data_min":1, "data_max":1, "data_q1":1, \
             "data_q2":1, "data_q3":1, "data_p95":1, "data_me":0, "data_num_data_points":1, "trial_id":"foo_3", "experiment_id":"foo", \
             "data_p90":1, "data_p99":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeExperimentMetrics(dataRDD, "data")
    result.update(computeModeMinMax(dataRDD, "data"))
    assert result["mode_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_min_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mode_max_freq"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_min"] == 1, "Experiment metric value incorrect, expected 1"
    assert result["mean_max"] == 1, "Experiment metric value incorrect, expected 1"
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
    assert result["best"] == ["foo_1", "foo_2", "foo_3"], "Experiment metric value incorrect, expected foo_1, foo_2, foo_3"
    assert result["worst"] == ["foo_1", "foo_2", "foo_3"], "Experiment metric value incorrect, expected foo_1, foo_2, foo_3"
    assert result["average"] == ["foo_1", "foo_2", "foo_3"], "Experiment metric value incorrect, expected foo_1, foo_2, foo_3"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testTwoElements(sc)
    testThreeElements(sc)
    testFourElements(sc)
    testAllSameElements(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()