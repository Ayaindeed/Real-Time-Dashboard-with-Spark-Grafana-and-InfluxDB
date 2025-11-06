import sys
import os
import json
import time
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producers.event_producer import EventProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_event_generation():
    """Test event generation functionality"""
    logger.info("Testing event generation...")
    
    try:
        producer = EventProducer()
        
        # Test click event generation
        click_event = producer.generate_click_event()
        logger.info(f"Generated click event: {json.dumps(click_event, indent=2)}")
        
        # Validate click event structure
        required_click_fields = ['event_type', 'user_id', 'campaign_id', 'timestamp']
        for field in required_click_fields:
            assert field in click_event, f"Missing field: {field}"
        assert click_event['event_type'] == 'click'
        
        # Test purchase event generation
        purchase_event = producer.generate_purchase_event()
        logger.info(f"Generated purchase event: {json.dumps(purchase_event, indent=2)}")
        
        # Validate purchase event structure
        required_purchase_fields = ['event_type', 'user_id', 'campaign_id', 'order_id', 'total_amount', 'timestamp']
        for field in required_purchase_fields:
            assert field in purchase_event, f"Missing field: {field}"
        assert purchase_event['event_type'] == 'purchase'
        assert purchase_event['total_amount'] > 0
        
        logger.info("✅ Event generation test passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Event generation test failed: {e}")
        return False

def test_kafka_connection():
    """Test Kafka connection"""
    logger.info("Testing Kafka connection...")
    
    try:
        producer = EventProducer()
        
        # Test if producer can be created
        assert producer.producer is not None, "Producer not created"
        
        logger.info("✅ Kafka connection test passed!")
        producer.producer.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka connection test failed: {e}")
        logger.error("Make sure Kafka is running on localhost:9092")
        return False

def test_batch_production():
    """Test batch event production"""
    logger.info("Testing batch event production...")
    
    try:
        producer = EventProducer()
        
        # Generate small batch for testing
        test_events = 10
        start_time = time.time()
        
        success_count = 0
        for i in range(test_events):
            event = producer.generate_event()
            if producer.send_event(event):
                success_count += 1
                logger.info(f"Event {i+1}/{test_events}: {event['event_type']} for user {event['user_id']}")
        
        producer.producer.flush()
        producer.producer.close()
        
        end_time = time.time()
        
        logger.info(f"✅ Batch production test completed!")
        logger.info(f"   Events generated: {test_events}")
        logger.info(f"   Events sent successfully: {success_count}")
        logger.info(f"   Success rate: {success_count/test_events*100:.1f}%")
        logger.info(f"   Time taken: {end_time-start_time:.2f}s")
        
        return success_count == test_events
        
    except Exception as e:
        logger.error(f"❌ Batch production test failed: {e}")
        return False

def test_event_distribution():
    """Test event type distribution"""
    logger.info("Testing event distribution...")
    
    try:
        producer = EventProducer()
        
        click_count = 0
        purchase_count = 0
        total_events = 100
        
        for _ in range(total_events):
            event = producer.generate_event()
            if event['event_type'] == 'click':
                click_count += 1
            elif event['event_type'] == 'purchase':
                purchase_count += 1
        
        click_percentage = click_count / total_events
        purchase_percentage = purchase_count / total_events
        
        logger.info(f"Event distribution over {total_events} events:")
        logger.info(f"   Clicks: {click_count} ({click_percentage:.1%})")
        logger.info(f"   Purchases: {purchase_count} ({purchase_percentage:.1%})")
        
        # Should be approximately 80% clicks, 20% purchases
        assert 0.7 <= click_percentage <= 0.9, f"Click percentage out of range: {click_percentage}"
        assert 0.1 <= purchase_percentage <= 0.3, f"Purchase percentage out of range: {purchase_percentage}"
        
        logger.info("✅ Event distribution test passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Event distribution test failed: {e}")
        return False

def run_performance_test():
    """Run performance test"""
    logger.info("Running performance test...")
    
    try:
        producer = EventProducer()
        
        events_per_second = 50
        test_duration = 10  # seconds
        total_events = events_per_second * test_duration
        
        logger.info(f"Generating {events_per_second} events/second for {test_duration} seconds...")
        
        start_time = time.time()
        success_count = 0
        
        for i in range(total_events):
            event = producer.generate_event()
            if producer.send_event(event):
                success_count += 1
            
            # Control rate
            expected_time = start_time + (i + 1) / events_per_second
            current_time = time.time()
            if current_time < expected_time:
                time.sleep(expected_time - current_time)
        
        producer.producer.flush()
        producer.producer.close()
        
        end_time = time.time()
        actual_duration = end_time - start_time
        actual_rate = success_count / actual_duration
        
        logger.info(f"✅ Performance test completed!")
        logger.info(f"   Target rate: {events_per_second} events/second")
        logger.info(f"   Actual rate: {actual_rate:.1f} events/second")
        logger.info(f"   Success rate: {success_count/total_events*100:.1f}%")
        logger.info(f"   Duration: {actual_duration:.2f}s")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Performance test failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("🚀 Starting Event Producer Tests")
    logger.info("=" * 50)
    
    tests = [
        ("Event Generation", test_event_generation),
        ("Kafka Connection", test_kafka_connection),
        ("Batch Production", test_batch_production),
        ("Event Distribution", test_event_distribution),
        ("Performance Test", run_performance_test)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name} Test...")
        logger.info("-" * 30)
        
        try:
            result = test_func()
            results.append((test_name, result))
            
            if result:
                logger.info(f"✅ {test_name} test PASSED")
            else:
                logger.error(f"❌ {test_name} test FAILED")
        except Exception as e:
            logger.error(f"💥 {test_name} test CRASHED: {e}")
            results.append((test_name, False))
    
    # Print summary
    logger.info("\n" + "=" * 50)
    logger.info("📊 TEST SUMMARY")
    logger.info("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"   {test_name}: {status}")
    
    logger.info("-" * 50)
    logger.info(f"   Total: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Producer is ready.")
        return 0
    else:
        logger.error(f"💔 {total - passed} tests failed. Check logs above.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
