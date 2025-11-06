import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import yaml
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProducer:
    def __init__(self, config_file=None):
        """Initialize the event producer with configuration"""
        self.config = self.load_config(config_file)
        self.producer = self.create_producer()
        self.user_ids = list(range(1, 10001))  # 10K users
        self.campaign_ids = ['SUMMER2024', 'WINTER2024', 'SPRING2024', 'FALL2024', 'BLACKFRIDAY', 'CYBER_MONDAY']
        
    def load_config(self, config_file):
        """Load configuration from file or environment"""
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        
        # Fallback to environment variables
        return {
            'kafka': {
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                'topic': os.getenv('KAFKA_TOPIC_EVENTS', 'user_events'),
                'client_id': 'event_producer',
                'value_serializer': 'json'
            },
            'producer': {
                'events_per_second': int(os.getenv('EVENT_GENERATION_INTERVAL', 1)),
                'batch_size': int(os.getenv('BATCH_SIZE', 100))
            }
        }
    
    def create_producer(self):
        """Create and configure Kafka producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                client_id=self.config['kafka']['client_id'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                retries=3,
                acks='all'
            )
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def generate_click_event(self):
        """Generate a user click event"""
        return {
            'event_type': 'click',
            'user_id': random.choice(self.user_ids),
            'campaign_id': random.choice(self.campaign_ids),
            'product_id': f"PROD_{random.randint(1, 1000)}",
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': f"sess_{random.randint(100000, 999999)}",
            'page_url': f"/product/{random.randint(1, 1000)}",
            'user_agent': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        }
    
    def generate_purchase_event(self):
        """Generate a user purchase event"""
        units = random.randint(1, 5)
        unit_price = round(random.uniform(10.0, 500.0), 2)
        total_amount = round(units * unit_price, 2)
        
        return {
            'event_type': 'purchase',
            'user_id': random.choice(self.user_ids),
            'campaign_id': random.choice(self.campaign_ids),
            'order_id': f"ORD_{random.randint(100000, 999999)}",
            'product_id': f"PROD_{random.randint(1, 1000)}",
            'units': units,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'currency': 'USD',
            'timestamp': datetime.utcnow().isoformat(),
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay']),
            'shipping_address': {
                'country': random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU']),
                'state': random.choice(['CA', 'NY', 'TX', 'FL', 'WA', 'ON', 'BC'])
            }
        }
    
    def generate_event(self):
        """Generate a random event (80% clicks, 20% purchases)"""
        if random.random() < 0.8:
            return self.generate_click_event()
        else:
            return self.generate_purchase_event()
    
    def send_event(self, event):
        """Send event to Kafka"""
        try:
            future = self.producer.send(
                self.config['kafka']['topic'],
                key=str(event['user_id']),
                value=event
            )
            
            # Optional: wait for confirmation
            record_metadata = future.get(timeout=10)
            logger.debug(f"Event sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending event: {e}")
            return False
    
    def run_continuous(self):
        """Run continuous event generation"""
        logger.info(f"Starting event generation to topic: {self.config['kafka']['topic']}")
        logger.info(f"Events per second: {self.config['producer']['events_per_second']}")
        
        try:
            while True:
                start_time = time.time()
                events_sent = 0
                
                for _ in range(self.config['producer']['events_per_second']):
                    event = self.generate_event()
                    if self.send_event(event):
                        events_sent += 1
                
                elapsed_time = time.time() - start_time
                sleep_time = max(0, 1 - elapsed_time)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                logger.info(f"Sent {events_sent} events in {elapsed_time:.2f}s")
                
        except KeyboardInterrupt:
            logger.info("Stopping event generation...")
        except Exception as e:
            logger.error(f"Error in continuous generation: {e}")
        finally:
            self.producer.close()
            logger.info("Producer closed")
    
    def run_batch(self, num_events=100):
        """Run batch event generation for testing"""
        logger.info(f"Generating {num_events} events...")
        
        try:
            for i in range(num_events):
                event = self.generate_event()
                if self.send_event(event):
                    logger.info(f"Event {i+1}/{num_events} sent: {event['event_type']} for user {event['user_id']}")
                else:
                    logger.error(f"Failed to send event {i+1}")
            
            # Flush to ensure all messages are sent
            self.producer.flush()
            logger.info(f"Successfully sent {num_events} events")
            
        except Exception as e:
            logger.error(f"Error in batch generation: {e}")
        finally:
            self.producer.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='E-commerce Event Producer')
    parser.add_argument('--mode', choices=['continuous', 'batch'], default='continuous',
                        help='Run mode: continuous or batch')
    parser.add_argument('--count', type=int, default=100,
                        help='Number of events for batch mode')
    parser.add_argument('--config', type=str,
                        help='Path to configuration file')
    
    args = parser.parse_args()
    
    producer = EventProducer(args.config)
    
    if args.mode == 'continuous':
        producer.run_continuous()
    else:
        producer.run_batch(args.count)

if __name__ == "__main__":
    main()
