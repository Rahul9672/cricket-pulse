import json
from kafka import KafkaConsumer
from collections import defaultdict

KAFKA_TOPIC = 'ipl-balls'
KAFKA_BROKER = 'localhost:9092'

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BROKER,

        # group_id = consumer ka naam/group
        # same group_id = messages divide honge
        # alag group_id = sab messages dobara milenge

        group_id = 'cricket-pulse-consumer',

        # earliest = topic ki shuruat se padho
        # latest = sirf naye messages padho

        auto_offset_reset = 'earliest',

        # bytes → Python dict
        value_deserializer = lambda v : json.loads(v.decode('utf-8')),
        key_deserializer = lambda k : k.decode('utf-8') if k else None,

        # kitni der wait karo agar koi message nahi
        consumer_timeout_ms = 5000 # 5 sec
    )

def consume_and_display():
    """
      kafka se event ball padho 
      live stats compute kro 
      console pr dikhao
    """
    consumer = create_consumer()

    # Stats track karo
    run_per_batter = defaultdict(int)
    ball_per_batter = defaultdict(int)
    wickets = []
    total_balls = 0

    print("\n CricketPulse - live ball feed")
    print("="* 60)

    for message in consumer:
        event = message.value
        total_balls += 1

        batter = event.get('batter','unknown')
        run = event.get('runs_off_bat',0)
        wicket = event.get('wicket')
        over = event.get('over',0)
        ball = event.get('ball',0)

        # Stats update karo
        run_per_batter[batter] += run
        ball_per_batter[batter] += 1

        if wicket:
            wickets.append({
                'batter' :batter,
                'kind' : wicket,
                'over' : f"{over}.{ball}"
            })
        # Har ball print karo
        wickets_str = f"WICKET: {wickets}! " if wicket else ""

        print(
            f"  Over {over}.{ball:<2} | "
            f"{batter:<22} | "
            f"Runs: {run} | "
            f"Total: {event.get('total_runs', 0)}"
            f"{wickets_str}"
        )
        # Har 30 balls pe mini scorecard dikhao

        if total_balls % 30 == 0:
            print("\n" + "─" * 60)
            print(f"  After {total_balls} balls — Top Batters:")
            sorted_batters = sorted(
                run_per_batter.items(),
                key=lambda x: x[1],
                reverse=True
            )[:3]
            for name, r in sorted_batters:
                b = ball_per_batter[name]
                sr = round((r / b) * 100, 1) if b > 0 else 0
                print(f"    {name:<22} {r:>3} runs  ({b} balls)  SR: {sr}")
            print("─" * 60 + "\n")
        # Match khatam — final scorecard
    print("\n" + "=" * 60)
    print(" FINAL SCORECARD")
    print("=" * 60)
    
    print("\n Batting Summary:")
    for name, r in sorted(run_per_batter.items(), key=lambda x: x[1], reverse=True):
        b = ball_per_batter[name]
        sr = round((r / b) * 100, 1) if b > 0 else 0
        print(f"  {name:<25} {r:>4} runs  ({b:>3} balls)  SR: {sr}")
    
    print(f"\n Wickets fallen: {len(wickets)}")
    for w in wickets:
        print(f"  Over {w['over']} — {w['batter']} — {w['kind']}")
    
    print(f"\nTotal deliveries consumed: {total_balls}")
    consumer.close()

if __name__ == "__main__":
    consume_and_display()