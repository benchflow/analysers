from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

#Test with no data
def testEmpty(sc):
    from databaseSize import databaseSize
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = databaseSize(dataRDD)
    assert result is None, "Size value incorrect, expected None"

#Test for data with one element  
def testOneElement(sc):
    from databaseSize import databaseSize
    
    data = [{"size":10}]
    
    dataRDD = sc.parallelize(data)
        
    result = databaseSize(dataRDD)
    assert result == 10, "Size value incorrect, expected 10"

#Test for data with two elements  
def testTwoElements(sc):
    from databaseSize import databaseSize
    
    data = [{"size":10}, {"size":20}]
    
    dataRDD = sc.parallelize(data)
        
    result = databaseSize(dataRDD)
    assert result == 30, "Size value incorrect, expected 30"

#Test for data with large elements    
def testLargeElement(sc):
    from databaseSize import databaseSize
    
    data = [{"size":9000000}, {"size":9000000}]
    
    dataRDD = sc.parallelize(data)
        
    result = databaseSize(dataRDD)
    assert result == 18000000, "Size value incorrect, expected 18000000"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testTwoElements(sc)
    testLargeElement(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()