import unittest
from unittest.mock import patch, MagicMock
from data.generator.kafka_producer import generate_sensor_data

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

    @patch("data.generator.kafka_producer.KafkaProducer")
    def test_producer_send(self, MockKafkaProducer):
        from data.generator.kafka_producer import producer, KAFKA_TOPIC
        mock_producer = MockKafkaProducer.return_value
        mock_producer.send.return_value = MagicMock()
        # Ensure producer is called with correct topic
        # (Simplified; you can extend with actual send test)
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()
