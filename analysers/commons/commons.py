import json
import gzip
import math
import scipy.integrate as integrate
import scipy.stats as stats
import scipy.special as special
import numpy as np
import yaml

def getHostCores(sc, cassandraKeyspace, hostID):
    hostProperties = sc.cassandraTable(cassandraKeyspace, "host_properties") \
                .select("n_cpu") \
                .where("host_id=?", hostID) \
                .first()
                
    nOfCores = hostProperties["n_cpu"]
    
    return nOfCores

def computeMetrics(data):
    if len(data) == 0:
        return {"median":None, "mean":None, "integral":None, "num_data_points":0, \
              "min":None, "max":None, "sd":None, "q1":None, "q2":None, "q3":None, "p90":None, "p95":None, "p99":None, "me":None, \
              "ci095_min":None, "ci095_max":None, "percentiles":None}   
    
    dataMin = np.min(data).item()
    dataMax = np.max(data).item()
    dataLength = len(data)
    percentilesIndexes = range(0, 101)
    percentilesNp = np.percentile(data, percentilesIndexes)
    percentiles = []
    for p in percentilesNp:
        percentiles.append(p.item())
    q1 = percentiles[25]
    q2 = percentiles[50]
    q3 = percentiles[75]
    p95 = percentiles[95]
    p90 = percentiles[90]
    p99 = percentiles[99]
    mean = np.mean(data, dtype=np.float64).item()
    variance = np.var(data, dtype=np.float64).item()
    stdD = np.std(data, dtype=np.float64).item()
    stdE = stdD/float(math.sqrt(dataLength))
    marginError = stdE * 2
    CILow = mean - marginError
    CIHigh = mean + marginError
    dataIntegral = integrate.trapz(data).item()

    return {"mean":mean, "integral":dataIntegral, "num_data_points":dataLength, \
              "min":dataMin, "max":dataMax, "sd":stdD, "q1":q1, "q2":q2, "q3":q3, "p95":p95, "me":marginError, \
              "ci095_min":CILow, "ci095_max":CIHigh, "p90":p90, "p99":p99, "percentiles": percentiles}
    
def computeExperimentMetrics(CassandraRDD, dataName):
    if CassandraRDD.isEmpty():
        return {"median_min":None, "median_max":None, \
              "mean_min":None, "mean_max":None, \
              "min":None, "max":None, "q1_min":None, \
              "q1_max":None, "q2_min":None, "q2_max":None, \
              "p95_max":None, "p95_min":None, \
              "p90_max":None, "p90_min":None, \
              "p99_max":None, "p99_min":None, \
              "q3_min":None, "q3_max":None, "weighted_avg":None, \
              "best": None, "worst": None, "average": None, \
              "variation_coefficient": None}
    
    def sortAndGet(CassandraRDD, field, asc):
        if asc == 1:
            v = CassandraRDD.map(lambda x: x[field]) \
                .min()
        else:
            v = CassandraRDD.map(lambda x: x[field]) \
                .max()
        return v
    
    dataMin = sortAndGet(CassandraRDD, dataName+"_min", 1)
    dataMax = sortAndGet(CassandraRDD, dataName+"_max", 0)
    q1Min = sortAndGet(CassandraRDD, dataName+"_q1", 1)
    q1Max = sortAndGet(CassandraRDD, dataName+"_q1", 0)
    q2Min = sortAndGet(CassandraRDD, dataName+"_q2", 1)
    q2Max = sortAndGet(CassandraRDD, dataName+"_q2", 0)
    q3Min = sortAndGet(CassandraRDD, dataName+"_q3", 1)
    q3Max = sortAndGet(CassandraRDD, dataName+"_q3", 0)
    p90Min = sortAndGet(CassandraRDD, dataName+"_p90", 1)
    p90Max = sortAndGet(CassandraRDD, dataName+"_p90", 0)
    p95Min = sortAndGet(CassandraRDD, dataName+"_p95", 1)
    p95Max = sortAndGet(CassandraRDD, dataName+"_p95", 0)
    p99Min = sortAndGet(CassandraRDD, dataName+"_p99", 1)
    p99Max = sortAndGet(CassandraRDD, dataName+"_p99", 0)
    
    means = CassandraRDD.map(lambda a: a[dataName+'_mean']).collect()
    coefficientOfVariation = stats.variation(means).item()*100
    
    weightSum = CassandraRDD.map(lambda x: x[dataName+"_num_data_points"]) \
        .reduce(lambda a, b: a+b)
        
    weightedSum = CassandraRDD.map(lambda x: x[dataName+"_mean"]*x[dataName+"_num_data_points"]) \
        .reduce(lambda a, b: a+b)
    
    weightedMean = weightedSum/float(weightSum)

    meanMin = sortAndGet(CassandraRDD, dataName+"_mean", 1)
    meMin = CassandraRDD.filter(lambda x: x[dataName+"_mean"] == meanMin) \
        .map(lambda x: (x[dataName+"_me"], 0)) \
        .sortByKey(1, 1) \
        .map(lambda x: x[0]) \
        .first()
    bestTrials = CassandraRDD.filter(lambda x: x[dataName+"_mean"] == meanMin and x[dataName+"_me"] == meMin) \
        .map(lambda x: x["trial_id"]) \
        .collect()
    
    meanMax = sortAndGet(CassandraRDD, dataName+"_mean", 0)
    meMax = CassandraRDD.filter(lambda x: x[dataName+"_mean"] == meanMax) \
        .map(lambda x: (x[dataName+"_me"], 0)) \
        .sortByKey(0, 1) \
        .map(lambda x: x[0]) \
        .first()
    worstTrials = CassandraRDD.filter(lambda x: x[dataName+"_mean"] == meanMax and x[dataName+"_me"] == meMax) \
        .map(lambda x: x["trial_id"]) \
        .collect()
        
    meanAverage = CassandraRDD.map(lambda x: (x[dataName+"_mean"], 1)) \
        .reduce(lambda a, b: (a[0]+b[0],a[1]+b[1]))
    meanAverage = meanAverage[0]/float(meanAverage[1])
    meAverage = CassandraRDD.map(lambda x: (x[dataName+"_me"], 1)) \
        .reduce(lambda a, b: (a[0]+b[0],a[1]+b[1]))
    meAverage = meAverage[0]/float(meAverage[1])
    averageTrialsUpperMean = CassandraRDD.map(lambda x: (x[dataName+"_mean"], x["trial_id"])) \
        .sortByKey(1, 1) \
        .filter(lambda x: x[0] >= meanAverage) \
        .map(lambda x: x[0]) \
        .first()
    averageTrialsLowerMean = CassandraRDD.map(lambda x: (x[dataName+"_mean"], x["trial_id"])) \
        .sortByKey(0, 1) \
        .filter(lambda x: x[0] <= meanAverage) \
        .map(lambda x: x[0]) \
        .first()
    averageTrials = CassandraRDD.filter(lambda x: x[dataName+"_mean"] == averageTrialsUpperMean or x[dataName+"_mean"] == averageTrialsLowerMean) \
        .map(lambda x: x["trial_id"]) \
        .collect()
    
    # TODO: Fix this
    return {"mean_min":meanMin, "mean_max":meanMax, \
              "min":dataMin, "max":dataMax, "q1_min":q1Min, \
              "q1_max":q1Max, "q2_min":q2Min, "q2_max":q2Max, \
              "p90_max":p90Max, "p90_min":p90Min, \
              "p95_max":p95Max, "p95_min":p95Min, \
              "p99_max":p99Max, "p99_min":p99Min, \
              "q3_min":q3Min, "q3_max":q3Max, "weighted_avg":weightedMean, \
              "best": bestTrials, "worst": worstTrials, "average": averageTrials, \
              "variation_coefficient": coefficientOfVariation}

def computeLevene(sc, cassandraKeyspace, expTable, dataTable, experimentID, containerID, hostID, dataName):
    trials = sc.cassandraTable(cassandraKeyspace, expTable) \
            .select("trial_id") \
            .where("experiment_id=?", experimentID) \
            .map(lambda a: a["trial_id"]) \
            .distinct() \
            .collect()
    if len(trials) < 2:
        return {"levene_mean":None, "levene_median":None, "levene_trimmed":None, \
                "levene_mean_stat":None, "levene_median_stat":None, "levene_trimmed_stat":None}
    try:
        samples = []
        for trial in trials:
            sample = sc.cassandraTable(cassandraKeyspace, dataTable) \
                .select(dataName) \
                .where("trial_id=? AND experiment_id=? AND container_id=? AND host_id=?", trial, experimentID, containerID, hostID) \
                .map(lambda a: a[dataName]) \
                .collect()
            samples.append(sample)
        levResultMean = stats.levene(*samples, center='mean')
        levResultMedian = stats.levene(*samples, center='median')
        levResultTrimmed = stats.levene(*samples, center='trimmed')
        return {"levene_mean":levResultMean.pvalue.item(), "levene_median":levResultMedian.pvalue.item(), "levene_trimmed":levResultTrimmed.pvalue.item(), \
                "levene_mean_stat":levResultMean.statistic.item(), "levene_median_stat":levResultMedian.statistic.item(), "levene_trimmed_stat":levResultTrimmed.statistic.item()}
    except:
        print "Could not compute levene test for " + dataName
        return {"levene_mean":None, "levene_median":None, "levene_trimmed":None, \
                "levene_mean_stat":None, "levene_median_stat":None, "levene_trimmed_stat":None}
    
def computeMode(dataRDD):
    from pyspark_cassandra import CassandraSparkContext
    from pyspark_cassandra import RowFormat
    from pyspark import SparkConf
    
    if dataRDD.isEmpty():
        return (None, None)
    
    data = dataRDD.reduceByKey(lambda a, b: a + b) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(0, 1) \
            .collect()
        
    mode = list()
    highestCount = data[0][0]        
    for d in data:
        if d[0] == highestCount:
            mode.append(d[1])
        else:
            break
    return (mode, highestCount)

def computeModeMinMax(CassandraRDD, dataName):
    if CassandraRDD.isEmpty():
        return {"mode_min":None, "mode_max":None, \
              "mode_min_freq":None, "mode_max_freq":None}
    
    modeMinValues = CassandraRDD.map(lambda x: (min(x[dataName+"_mode"]), x[dataName+"_mode_freq"])) \
        .sortByKey(1, 1) \
        .map(lambda x: (x[0], x[1])) \
        .first()
    modeMin = modeMinValues[0]
    modeMinFreq = modeMinValues[1]
    
    modeMaxValues = CassandraRDD.map(lambda x: (max(x[dataName+"_mode"]), x[dataName+"_mode_freq"])) \
        .sortByKey(0, 1) \
        .map(lambda x: (x[0], x[1])) \
        .first()
    modeMax = modeMaxValues[0]
    modeMaxFreq = modeMaxValues[1]
    
    return {"mode_min":modeMin, "mode_max":modeMax, \
              "mode_min_freq":modeMinFreq, "mode_max_freq":modeMaxFreq}

def cutNInitialProcesses(dataRDD, nToIgnore):
    from pyspark_cassandra import CassandraSparkContext
    from pyspark_cassandra import RowFormat
    from pyspark import SparkConf
    
    if dataRDD.isEmpty():
        return []
    
    processes = dataRDD.map(lambda r: r["process_definition_id"]) \
            .distinct() \
            .collect()
    
    maxTime = None
    maxID = None
    for p in processes:
        time = dataRDD.filter(lambda r: r["process_definition_id"] == p) \
            .map(lambda r: (r["start_time"], r["source_process_instance_id"])) \
            .sortByKey(1, 1) \
            .take(nToIgnore)
        if len(time) < nToIgnore:
            continue
        else:
            time = time[-1]
        if maxTime is None or time[0] > maxTime:
            maxTime = time[0]
            defId = dataRDD.filter(lambda r: r["process_definition_id"] == p and r["start_time"] == maxTime) \
                .map(lambda r: (r["source_process_instance_id"], 0)) \
                .sortByKey(1, 1) \
                .first()
            maxID = defId[0]
    
    data = dataRDD.map(lambda r: (r["start_time"], r)) \
            .sortByKey(1, 1) \
            .map(lambda r: r[1]) \
            .collect()
    
    index = -1
    if maxID is not None:
        for i in range(len(data)):
            if data[i]["source_process_instance_id"] == maxID:
                index = i
                break
    
    data = data[index+1:]
    return data