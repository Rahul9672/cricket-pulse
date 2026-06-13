import json
import time
import random
from kafka import KafkaProducer
from cricsheet_parser import load_match, extract_deliveries, get_all_match_files

KAFKA_TOPIC = 'ipl-balls'
KAFKA_BROKER = 'localhost:9092'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def simulate_live_match(match_filepath: str, delay: float = 1.0):
    """
    Simulate a live IPL match by publishing ball events to Kafka
    delay = seconds between each ball (1.0 = real match pace)
    """
    producer = create_producer()
    match = load_match(match_filepath)
    match_name = f"{match['info']['teams'][0]} vs {match['info']['teams'][1]}"
    
    print(f"\n LIVE: {match_name}")
    print(f"Publishing to Kafka topic: {KAFKA_TOPIC}")
    print("-" * 50)
    
    ball_count = 0
    for event in extract_deliveries(match):
        # Kafka key = match_id (ensures same match goes to same partition)
        producer.send(
            topic=KAFKA_TOPIC,
            key=event['match_id'],
            value=event
        )
        
        ball_count += 1
        over_ball = f"{event['over']}.{event['ball']}"
        wicket_str = f" WICKET: {event['wicket']}!" if event['wicket'] else ""
        
        print(f"  Over {over_ball:5} | {event['batter']:20} | "
              f"Runs: {event['runs_off_bat']} | "
              f"Total: {event['total_runs']}{wicket_str}")
        
        time.sleep(delay)  # simulate real match pace
    
    producer.flush()
    print(f"\nMatch complete. {ball_count} deliveries published.")

if __name__ == "__main__":
    # Install: pip install kafka-python
    files = get_all_match_files('../data')
    if not files:
        print("Download IPL data from cricsheet.org first")
    else:
        # Speed up for testing — 0.1 sec between balls
        simulate_live_match(str(files[0]), delay=0.1)