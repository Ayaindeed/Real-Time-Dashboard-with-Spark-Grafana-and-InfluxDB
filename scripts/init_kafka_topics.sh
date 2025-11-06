#!/bin/bash

# Kafka Topics Initialization Script
# This script creates the necessary Kafka topics for the streaming dashboard project

set -e

# Configuration
KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVERS="kafka:29092"
REPLICATION_FACTOR=1
PARTITIONS=3

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Kafka is ready
check_kafka_ready() {
    print_info "Checking if Kafka is ready..."
    
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --list > /dev/null 2>&1; then
            print_success "Kafka is ready!"
            return 0
        fi
        
        print_info "Attempt $attempt/$max_attempts: Kafka not ready yet, waiting 10 seconds..."
        sleep 10
        ((attempt++))
    done
    
    print_error "Kafka failed to become ready after $max_attempts attempts"
    return 1
}

# Function to create a topic
create_topic() {
    local topic_name=$1
    local partitions=${2:-$PARTITIONS}
    local replication_factor=${3:-$REPLICATION_FACTOR}
    
    print_info "Creating topic: $topic_name"
    
    # Check if topic already exists
    if docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --list | grep -q "^$topic_name$"; then
        print_warning "Topic '$topic_name' already exists, skipping creation"
        return 0
    fi
    
    # Create the topic
    if docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --create \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication_factor; then
        print_success "Topic '$topic_name' created successfully"
    else
        print_error "Failed to create topic '$topic_name'"
        return 1
    fi
}

# Function to describe a topic
describe_topic() {
    local topic_name=$1
    
    print_info "Describing topic: $topic_name"
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --describe \
        --topic $topic_name
}

# Function to list all topics
list_topics() {
    print_info "Listing all topics:"
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --list
}

# Main execution
main() {
    print_info "Starting Kafka topics initialization..."
    
    # Check if Kafka container is running
    if ! docker ps | grep -q $KAFKA_CONTAINER; then
        print_error "Kafka container '$KAFKA_CONTAINER' is not running"
        print_info "Please start the Docker Compose services first:"
        print_info "docker-compose -f docker/docker-compose.yml up -d"
        exit 1
    fi
    
    # Wait for Kafka to be ready
    if ! check_kafka_ready; then
        print_error "Kafka is not ready, aborting topic creation"
        exit 1
    fi
    
    # Create topics
    print_info "Creating Kafka topics..."
    
    # User events topic (main stream)
    create_topic "user_events" 6 1
    
    # Processed events topic (enriched data)
    create_topic "processed_events" 6 1
    
    # Analytics topics
    create_topic "user_analytics" 3 1
    create_topic "campaign_analytics" 3 1
    create_topic "product_analytics" 3 1
    
    # Error handling topic
    create_topic "error_events" 3 1
    
    # Dead letter topic
    create_topic "dead_letter_queue" 3 1
    
    # Kafka Connect topics (for InfluxDB connector)
    create_topic "connect-configs" 1 1
    create_topic "connect-offsets" 25 1
    create_topic "connect-status" 5 1
    
    print_info "Topic creation completed!"
    
    # List all topics
    echo
    list_topics
    
    # Describe key topics
    echo
    print_info "Topic details:"
    describe_topic "user_events"
    echo
    describe_topic "processed_events"
    
    print_success "Kafka topics initialization completed successfully!"
    
    # Print usage information
    echo
    print_info "Usage information:"
    print_info "- Producer should publish to 'user_events' topic"
    print_info "- Spark Streaming will consume from 'user_events' and publish to 'processed_events'"
    print_info "- Kafka Connect will consume from 'processed_events' and write to InfluxDB"
    print_info "- Analytics topics can be used for specific aggregations"
    
    echo
    print_info "Monitor topics with:"
    print_info "docker exec $KAFKA_CONTAINER kafka-console-consumer --bootstrap-server $BOOTSTRAP_SERVERS --topic user_events --from-beginning"
}

# Handle script arguments
case "${1:-}" in
    "list")
        list_topics
        ;;
    "describe")
        if [ -z "$2" ]; then
            print_error "Please specify a topic name: $0 describe <topic_name>"
            exit 1
        fi
        describe_topic "$2"
        ;;
    "create")
        if [ -z "$2" ]; then
            print_error "Please specify a topic name: $0 create <topic_name> [partitions] [replication_factor]"
            exit 1
        fi
        create_topic "$2" "$3" "$4"
        ;;
    "check")
        check_kafka_ready
        ;;
    "")
        main
        ;;
    *)
        print_info "Usage: $0 [list|describe <topic>|create <topic> [partitions] [replication]|check]"
        print_info "       $0              - Initialize all topics"
        print_info "       $0 list         - List all topics"
        print_info "       $0 describe     - Describe a specific topic"
        print_info "       $0 create       - Create a specific topic"
        print_info "       $0 check        - Check if Kafka is ready"
        exit 1
        ;;
esac
