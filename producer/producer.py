import json
import time
import random
import os
from datetime import datetime
from confluent_kafka import Producer, KafkaError, KafkaException
import confluent_kafka.admin as admin

# Configuration
KAFKA_TOPIC = 'hospital_vitals'
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:9092')
NUM_BEDS = 100  
PUSH_INTERVAL = 0.5

def ensure_topic_exists():
    """Create the topic if it doesn't exist"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conf = {'bootstrap.servers': KAFKA_SERVER}
            admin_client = admin.AdminClient(conf)
            
            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)
            if KAFKA_TOPIC in metadata.topics:
                print(f"✓ Topic '{KAFKA_TOPIC}' already exists")
                return True
            
            # Create topic
            new_topic = admin.NewTopic(
                KAFKA_TOPIC,
                num_partitions=3,
                replication_factor=1
            )
            
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()  # Wait for creation
                    print(f"✓ Topic '{KAFKA_TOPIC}' created successfully")
                except Exception as e:
                    print(f"⚠ Error creating topic: {e}")
            
            return True
        except Exception as e:
            print(f"⚠ Attempt {attempt + 1}/{max_retries} - Could not create topic: {e}")
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                print("✗ Failed to create topic after max retries")
                return False

def delivery_report(err, msg):
    """Callback for message delivery"""
    if err is not None:
        print(f"✗ Message delivery failed: {err}")
    else:
        # Success - optionally print debug info
        pass

def create_producer():
    """Initialize Kafka producer with retry logic"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            conf = {
                'bootstrap.servers': KAFKA_SERVER,
                'client.id': 'hospital_producer',
                'acks': 'all',
                'retries': 3,
                'enable.idempotence': True
            }
            
            producer = Producer(conf)
            print(f"✓ Connected to Kafka at {KAFKA_SERVER}")
            return producer
        except Exception as e:
            retry_count += 1
            print(f"⚠ Failed to connect to Kafka ({retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                time.sleep(5)
            else:
                print("✗ Failed to connect after max retries")
                raise

def get_sensor_reading(bed_id):
    """Generate sensor data with occasional anomalies"""
    data = {
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "bed_id": bed_id,
        "oxygen_sat": round(random.normalvariate(98, 1), 1),
        "body_temp": round(random.normalvariate(37, 0.3), 1),
        "heart_rate": int(random.normalvariate(75, 10)),
        "blood_pressure_sys": int(random.normalvariate(120, 5)),
        "glucose": round(random.uniform(70, 140), 1),
        "respiration_rate": int(random.normalvariate(16, 2))
    }

    # ANOMALY INJECTION: 1% chance of a critical event
    if random.random() < 0.01:
        anomaly_choice = random.choice(['hypoxia', 'fever', 'tachycardia'])
        if anomaly_choice == 'hypoxia':
            data["oxygen_sat"] = round(random.uniform(80, 89), 1)
        elif anomaly_choice == 'fever':
            data["body_temp"] = round(random.uniform(39, 41), 1)
        elif anomaly_choice == 'tachycardia':
            data["heart_rate"] = random.randint(150, 200)
            
    return data

# Main execution
print("Starting Hospital Vitals Producer...")
print(f"Using confluent-kafka library")

producer = create_producer()
ensure_topic_exists()

print(f"Starting Kafka Producer with {NUM_BEDS} beds...")
message_count = 0

try:
    while True:
        # Push data for all beds
        for bed_num in range(1, NUM_BEDS + 1):
            bed_id = f"BED_{bed_num:05d}"
            payload = get_sensor_reading(bed_id)
            
            # Produce message
            try:
                producer.produce(
                    KAFKA_TOPIC,
                    key=bed_id,
                    value=json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )
                message_count += 1
            except KafkaException as e:
                print(f"✗ Error sending message for {bed_id}: {e}")
        
        # Flush to ensure messages are sent
        producer.flush(timeout=10)
        print(f"✓ Sent {message_count} messages ({NUM_BEDS} beds)")
        message_count = 0
        time.sleep(PUSH_INTERVAL)
        
except KeyboardInterrupt:
    print("\n✓ Shutting down producer...")
    producer.flush()
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    raise