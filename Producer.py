from kafka import KafkaProducer
import time

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Stream data to Kafka topic
def produce_data(file_path):
    try:
        with open(file_path, 'r') as file:
            for line in file:
                producer.send('text-big', value=line.encode('utf-8'))
                time.sleep(1)  # Simulate real-time streaming
                print(f"Sent: {line.strip()}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()  # Ensure the producer is closed after use

# Replace 'Simple.txt' with your actual file
produce_data('Simple.txt')
