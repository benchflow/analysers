from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

def testEmpty(sc):
    from commons import computeMode
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = computeMode(dataRDD)
    assert result[0] is None, "Mode value incorrect, expected None"
    assert result[1] is None, "Mode frequency incorrect, expected None"
    
def testOneElement(sc):
    from commons import computeMode
    
    data = [(1, 1)]
    
    dataRDD = sc.parallelize(data)
        
    result = computeMode(dataRDD)
    assert result[0][0] == 1, "Mode value incorrect, expected 1"
    assert result[1] == 1, "Mode frequency incorrect, expected 1"

def testSingleValueMode(sc):
    from commons import computeMode
    
    data = [(2, 1), (1, 1), (1, 1)]
    
    dataRDD = sc.parallelize(data)
        
    result = computeMode(dataRDD)
    assert result[0][0] == 1, "Mode value incorrect, expected 1"
    assert result[1] == 2, "Mode frequency incorrect, expected 2"
    
def testMultipleValuesMode(sc):
    from commons import computeMode
    
    data = [(2, 1), (1, 1)]
    
    dataRDD = sc.parallelize(data)
        
    result = computeMode(dataRDD)
    assert 2 in result[0], "Mode value incorrect, expected 2, 1"
    assert 1 in result[0], "Mode value incorrect, expected 2, 1"
    assert result[1] == 1, "Mode frequency incorrect, expected 1"
    
def testLargeMode(sc):
    from commons import computeMode
    
    data = []
    data.append((1,1))
    data.append((3,1))
    for i in range(1000000):
        data.append((2,1))
    
    dataRDD = sc.parallelize(data)
        
    result = computeMode(dataRDD)
    assert 2 in result[0], "Mode value incorrect, expected 2"
    assert result[1] == 1000000, "Mode frequency incorrect, expected 1000000"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testSingleValueMode(sc)
    testMultipleValuesMode(sc)
    testLargeMode(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()