import unittest
from processing.streaming.streaming_anomaly_job import score_anomaly

class TestAnomalyScoring(unittest.TestCase):
    def test_score_anomaly_high_vibration(self):
        row = {"avg_vib": 4.5}
        self.assertEqual(score_anomaly(row), 1.0)

    def test_score_anomaly_normal_vibration(self):
        row = {"avg_vib": 1.2}
        self.assertEqual(score_anomaly(row), 0.0)

if __name__ == "__main__":
    unittest.main()
