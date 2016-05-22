from pyspark_cassandra import CassandraSparkContext
from pyspark_cassandra import RowFormat
from pyspark import SparkConf

def testEmpty(sc):
    from commons import cutNInitialProcesses
    
    data = []
    
    dataRDD = sc.parallelize(data)
        
    result = cutNInitialProcesses(dataRDD, 1)
    assert (len(result) == 0), "Wrong number of cut processes"
    
def testOneElement(sc):
    from commons import cutNInitialProcesses
    
    data = [{"process_definition_id":"foobar", "source_process_instance_id":"foobar", "start_time":1, "duration":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = cutNInitialProcesses(dataRDD, 2)
    assert (len(result) == 1), "Wrong number of cut processes, expected 1"
    
def testTwoProcessesCutOne(sc):
    from commons import cutNInitialProcesses
    
    data = [{"process_definition_id":"foo", "source_process_instance_id":"foo", "start_time":1, "duration":1}, \
            {"process_definition_id":"foo", "source_process_instance_id":"foo", "start_time":1, "duration":1}, \
            {"process_definition_id":"bar", "source_process_instance_id":"bar", "start_time":2, "duration":1}, \
            {"process_definition_id":"bar", "source_process_instance_id":"bar", "start_time":2, "duration":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = cutNInitialProcesses(dataRDD, 1)
    assert (len(result) == 1), "Wrong number of cut processes, expected 1"
    
def testOneProcessCutOne(sc):
    from commons import cutNInitialProcesses
    
    data = [{"process_definition_id":"foo", "source_process_instance_id":"foo", "start_time":1, "duration":1}, \
            {"process_definition_id":"bar", "source_process_instance_id":"bar", "start_time":1, "duration":1}]
    
    dataRDD = sc.parallelize(data)
    
    result = cutNInitialProcesses(dataRDD, 1)
    assert (len(result) == 1), "Wrong number of cut processes, expected 1"
    
def testOneProcessCutTwo(sc):
    from commons import cutNInitialProcesses
    
    data = [{"process_definition_id":"foo", "source_process_instance_id":"foo", "start_time":1, "duration":1}, \
            {"process_definition_id":"bar", "source_process_instance_id":"bar", "start_time":1, "duration":1}]
    
    dataRDD = sc.parallelize(data)
        
    result = cutNInitialProcesses(dataRDD, 2)
    assert (len(result) == 2), "Wrong number of cut processes, expected 2"
           
def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local")
    sc = CassandraSparkContext(conf=conf)
    
    testEmpty(sc)
    testOneElement(sc)
    testTwoProcessesCutOne(sc)
    testOneProcessCutOne(sc)
    testOneProcessCutTwo(sc)
    print("All tests passed")

if __name__ == '__main__':
    main()