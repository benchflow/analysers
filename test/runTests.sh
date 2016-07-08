#!/bin/sh

SPARK_MASTER=local[*]
PYSPARK_CASSANDRA_JAR_PATH=/test/dependencies/pyspark-cassandra-assembly-0.3.5.jar
TRIAL_ID=camundaZZZZZZMV_ZZZZZZOX
EXPERIMENT_ID=camundaZZZZZZMV
SUT_NAME=camunda
CONTAINER_ID=stats_camunda
CASSANDRA_HOST=cassandra
HOST_NAME=docker_host

echo "Starting Python tests"

python2.7 /test/pythonTests/computeMetricsTest.py

echo "Starting Spark tests"

for SCRIPT in "computeModeTest" "cutNInitialProcessesTest" "computeExperimentsMetricsTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--py-files $ANALYSERS_PATH/commons/commons.py,$PYSPARK_CASSANDRA_JAR_PATH \
	/test/sparkTests/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

for SCRIPT in "databaseSizeTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--py-files $ANALYSERS_PATH/trials/databaseSize.py,$PYSPARK_CASSANDRA_JAR_PATH \
	/test/sparkTests/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

for SCRIPT in "IOTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--py-files $ANALYSERS_PATH/trials/IO.py,$PYSPARK_CASSANDRA_JAR_PATH \
	/test/sparkTests/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

for SCRIPT in "computeExperimentCoreMetricsTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--py-files $ANALYSERS_PATH/experiments/cpu.py,$PYSPARK_CASSANDRA_JAR_PATH \
	/test/sparkTests/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

echo "Starting Cassandra tests"

for SCRIPT in "cpu" "ram" "IO" "databaseSize" "processDuration" "executionTime" "numberOfProcessInstances" "throughput"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
	--py-files $ANALYSERS_PATH/commons/commons.py,$PYSPARK_CASSANDRA_JAR_PATH \
	$ANALYSERS_PATH/trials/$SCRIPT.py \
	'{"cassandra_keyspace":"benchflow", "sut_name": "'$SUT_NAME'", "trial_id": "'$TRIAL_ID'", "experiment_id": "'$EXPERIMENT_ID'", "container_id": "'$CONTAINER_ID'", "host_id": "'$HOST_NAME'"}'
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

for SCRIPT in "cpu" "ram" "IO" "databaseSize" "processDuration" "executionTime" "numberOfProcessInstances" "throughput"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
	--py-files $ANALYSERS_PATH/commons/commons.py,$PYSPARK_CASSANDRA_JAR_PATH \
	$ANALYSERS_PATH/experiments/$SCRIPT.py \
	'{"cassandra_keyspace":"benchflow", "sut_name": "'$SUT_NAME'", "trial_id": "'$TRIAL_ID'", "experiment_id": "'$EXPERIMENT_ID'", "container_id": "'$CONTAINER_ID'", "host_id": "'$HOST_NAME'"}'
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

for SCRIPT in "cassandraTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA_JAR_PATH \
    --driver-class-path $PYSPARK_CASSANDRA_JAR_PATH \
	--py-files $ANALYSERS_PATH/commons/commons.py,$PYSPARK_CASSANDRA_JAR_PATH \
	/test/sparkTests/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
	echo $SCRIPT completed without errors
	sleep 5
done

exit 0