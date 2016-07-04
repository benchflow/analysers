import unittest
from commons import *

class MetricsTestCase(unittest.TestCase):
    def testEmpty(self):
        data = []
        metrics = computeMetrics(data)
        self.assertTrue(metrics["mean"] is None)
        self.assertTrue(metrics["integral"] is None)
        self.assertTrue(metrics["num_data_points"] == 0)
        self.assertTrue(metrics["min"] is None)
        self.assertTrue(metrics["max"] is None)
        self.assertTrue(metrics["sd"] is None)
        self.assertTrue(metrics["q1"] is None)
        self.assertTrue(metrics["q2"] is None)
        self.assertTrue(metrics["q3"] is None)
        self.assertTrue(metrics["p95"] is None)
        self.assertTrue(metrics["me"] is None)
        self.assertTrue(metrics["ci095_min"] is None)
        self.assertTrue(metrics["ci095_max"] is None)
        
    def testOneElement(self):
        data = [1]
        metrics = computeMetrics(data)
        self.assertTrue(metrics["mean"] == 1)
        self.assertTrue(metrics["integral"] == 0)
        self.assertTrue(metrics["num_data_points"] == 1)
        self.assertTrue(metrics["min"] == 1)
        self.assertTrue(metrics["max"] == 1)
        self.assertTrue(metrics["sd"] == 0)
        self.assertTrue(metrics["q1"] == 1)
        self.assertTrue(metrics["q2"] == 1)
        self.assertTrue(metrics["q3"] == 1)
        self.assertTrue(metrics["p95"] == 1)
        self.assertTrue(metrics["me"] == 0)
        self.assertTrue(metrics["ci095_min"] == 1)
        self.assertTrue(metrics["ci095_max"] == 1)
        
    def testEvenNumberOfElements(self):
        data = [2, 4, 2, 4]
        metrics = computeMetrics(data)
        self.assertTrue(metrics["mean"] == 3)
        self.assertTrue(metrics["integral"] == 9)
        self.assertTrue(metrics["num_data_points"] == 4)
        self.assertTrue(metrics["min"] == 2)
        self.assertTrue(metrics["max"] == 4)
        self.assertTrue(metrics["sd"] == 1)
        self.assertTrue(metrics["q1"] == 2)
        self.assertTrue(metrics["q2"] == 3)
        self.assertTrue(metrics["q3"] == 4)
        self.assertTrue(metrics["p95"] == 4)
        self.assertTrue(metrics["me"] == 1)
        self.assertTrue(metrics["ci095_min"] == 2)
        self.assertTrue(metrics["ci095_max"] == 4)
        
    def testOddNumberOfElements(self):
        data = [2, 2, 2]
        metrics = computeMetrics(data)
        self.assertTrue(metrics["mean"] == 2)
        self.assertTrue(metrics["integral"] == 4)
        self.assertTrue(metrics["num_data_points"] == 3)
        self.assertTrue(metrics["min"] == 2)
        self.assertTrue(metrics["max"] == 2)
        self.assertTrue(metrics["sd"] == 0)
        self.assertTrue(metrics["q1"] == 2)
        self.assertTrue(metrics["q2"] == 2)
        self.assertTrue(metrics["q3"] == 2)
        self.assertTrue(metrics["p95"] == 2)
        self.assertTrue(metrics["me"] == 0)
        self.assertTrue(metrics["ci095_min"] == 2)
        self.assertTrue(metrics["ci095_max"] == 2)
        
    def testLargeNumberOfElements(self):
        data = []
        for i in range(10000):
            data.append(2)
        metrics = computeMetrics(data)
        self.assertTrue(metrics["mean"] == 2)
        self.assertTrue(metrics["integral"] == 19998)
        self.assertTrue(metrics["num_data_points"] == 10000)
        self.assertTrue(metrics["min"] == 2)
        self.assertTrue(metrics["max"] == 2)
        self.assertTrue(metrics["sd"] == 0)
        self.assertTrue(metrics["q1"] == 2)
        self.assertTrue(metrics["q2"] == 2)
        self.assertTrue(metrics["q3"] == 2)
        self.assertTrue(metrics["p95"] == 2)
        self.assertTrue(metrics["me"] == 0)
        self.assertTrue(metrics["ci095_min"] == 2)
        self.assertTrue(metrics["ci095_max"] == 2)

if __name__ == '__main__':
    unittest.main()