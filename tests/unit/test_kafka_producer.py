import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from data.generator.kafka_producer import generate_sensor_data, get_producer

class TestKafkaProducer(unittest.TestCase):
    def test_generate_sensor_data_structure(self):
        data = generate_sensor_data()
        self.assertIn("vehicle_id", data)
        self.assertIn("temperature", data)
        self.assertIn("vibration", data)
        self.assertIn("rpm", data)
        self.assertIn("location", data)
        self.assertIsInstance(data["temperature"], float)
        self.assertGreaterEqual(data["temperature"], 70)
        self.assertLessEqual(data["temperature"], 120)

    @patch("data.generator.kafka_producer.create_producer")
    def test_get_producer_returns_producer(self, mock_create):
        mock_producer = MagicMock()
        mock_create.return_value = mock_producer
        producer = get_producer()
        self.assertEqual(producer, mock_producer)

if __name__ == "__main__":
    unittest.main()
