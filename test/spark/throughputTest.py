from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
import datetime

def testEmpty(sc):
    from throughput import computeThroughput
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = computeThroughput(dataRDD)
    assert result == (None, None), "Throughput value incorrect"
    
def testOneElement(sc):
    from throughput import computeThroughput
    
    data = [{"start_time":datetime.datetime.strptime("2016-03-18 11:05:24", "%Y-%m-%d %H:%M:%S"), "end_time":datetime.datetime.strptime("2016-03-18 11:05:25", "%Y-%m-%d %H:%M:%S")}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeThroughput(dataRDD)
    assert result == 1, "Throughput value incorrect, expected 1"
    
def testTwoElements(sc):
    from throughput import computeThroughput
    
    data = [{"start_time":datetime.datetime.strptime("2016-03-18 11:05:24", "%Y-%m-%d %H:%M:%S"), "end_time":datetime.datetime.strptime("2016-03-18 11:05:24", "%Y-%m-%d %H:%M:%S")}, \
            {"start_time":datetime.datetime.strptime("2016-03-18 11:05:20", "%Y-%m-%d %H:%M:%S"), "end_time":datetime.datetime.strptime("2016-03-18 11:02:22", "%Y-%m-%d %H:%M:%S")}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeThroughput(dataRDD)
    assert result == 0.5, "Throughput value incorrect, expected 0.5"
    
def testSomeNullTimes(sc):
    from throughput import computeThroughput
    
    data = [{"start_time":None, "end_time":datetime.datetime.strptime("2016-03-18 11:05:24", "%Y-%m-%d %H:%M:%S")}, \
            {"start_time":datetime.datetime.strptime("2016-03-18 11:05:20", "%Y-%m-%d %H:%M:%S"), "end_time": None}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeThroughput(dataRDD)
    assert result == 0.5, "Throughput value incorrect, expected 0.5"
    
def testAllTimesNull(sc):
    from throughput import computeThroughput
    
    data = [{"start_time":None, "end_time":datetime.datetime.strptime("2016-03-18 11:05:24", "%Y-%m-%d %H:%M:%S")}, \
            {"start_time":None, "end_time": None}]
    
    dataRDD = sc.parallelize(data)
        
    result = computeThroughput(dataRDD)
    assert result == None, "Throughput value incorrect, expected None"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testTwoElements(sc)
    testSomeNullTimes(sc)
    testAllTimesNull(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()