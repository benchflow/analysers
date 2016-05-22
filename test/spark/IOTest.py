from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def testEmpty(sc):
    from IO import maxIOValues
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = maxIOValues(dataRDD)
    assert len(result) == 0, "IO value incorrect, expected 0"
    
def testOneElement(sc):
    from IO import maxIOValues
    
    data = [{"device":"1", "reads":5, "writes":5, "total": 10}]
    
    dataRDD = sc.parallelize(data)
        
    result = maxIOValues(dataRDD)
    assert result[0]["device"] == "1", "IO value incorrect, expected '1'"
    assert result[0]["reads"] == 5, "IO value incorrect, expected 5"
    assert result[0]["writes"] == 5, "IO value incorrect, expected 5"
    assert result[0]["total"] == 10, "IO value incorrect, expected 10"
    
def testTwoElements(sc):
    from IO import maxIOValues
    
    data = [{"device":"1", "reads":5, "writes":5, "total": 10}, \
            {"device":"1", "reads":6, "writes":6, "total": 12}]
    
    dataRDD = sc.parallelize(data)
        
    result = maxIOValues(dataRDD)
    assert result[0]["device"] == "1", "IO value incorrect, expected '1'"
    assert result[0]["reads"] == 6, "IO value incorrect, expected 6"
    assert result[0]["writes"] == 6, "IO value incorrect, expected 6"
    assert result[0]["total"] == 12, "IO value incorrect, expected 12"
    
def testManyDevices(sc):
    from IO import maxIOValues
    
    data = [{"device":"1", "reads":5, "writes":5, "total": 10}, \
            {"device":"1", "reads":6, "writes":6, "total": 12}, \
            {"device":"2", "reads":8, "writes":8, "total": 16}, \
            {"device":"2", "reads":6, "writes":6, "total": 12}]
    
    dataRDD = sc.parallelize(data)
        
    result = maxIOValues(dataRDD)
    for d in result:
        assert "device" in d.keys()
        if d["device"] == "1":
            assert d["device"] == "1", "IO value incorrect, expected '1'"
            assert d["reads"] == 6, "IO value incorrect, expected 6"
            assert d["writes"] == 6, "IO value incorrect, expected 6"
            assert d["total"] == 12, "IO value incorrect, expected 12"
        if d["device"] == "2":
            assert d["device"] == "2", "IO value incorrect, expected '2'"
            assert d["reads"] == 8, "IO value incorrect, expected 8"
            assert d["writes"] == 8, "IO value incorrect, expected 8"
            assert d["total"] == 16, "IO value incorrect, expected 16"
            
def testNullValues(sc):
    from IO import maxIOValues
    
    data = [{"device":"1", "reads":5, "writes":None, "total": 5}, \
            {"device":"1", "reads":6, "writes":None, "total": 6}, \
            {"device":"2", "reads":None, "writes":8, "total": 8}, \
            {"device":"2", "reads":None, "writes":6, "total": 6}]
    
    dataRDD = sc.parallelize(data)
        
    result = maxIOValues(dataRDD)
    for d in result:
        assert "device" in d.keys()
        if d["device"] == "1":
            assert d["device"] == "1", "IO value incorrect, expected '1'"
            assert d["reads"] == 6, "IO value incorrect, expected 6"
            assert d["writes"] == None, "IO value incorrect, expected None"
            assert d["total"] == 6, "IO value incorrect, expected 6"
        if d["device"] == "2":
            assert d["device"] == "2", "IO value incorrect, expected '2'"
            assert d["reads"] == None, "IO value incorrect, expected None"
            assert d["writes"] == 8, "IO value incorrect, expected 8"
            assert d["total"] == 8, "IO value incorrect, expected 8"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testTwoElements(sc)
    testManyDevices(sc)
    testNullValues(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()