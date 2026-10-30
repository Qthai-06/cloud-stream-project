"""Generate IoT sensor events and publish them to Redpanda."""
import random
import json
import yaml
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer


def load_config(config_path = "configs/pipeline_config.yaml"):
    with open(file=config_path,mode='r',encoding="utf-8") as f:
        return yaml.safe_load(f)


def generator_sensor_event(devices, late_per, min_delay,max_delay):
    device_id = random.choice(devices)
    now = datetime.utcnow()

    is_late = random.random() < late_per
    if is_late:
        delay_sec = random.randint(min_delay,max_delay)
        event_time = now - timedelta(seconds=delay_sec)
    else:
        event_time = now

    event = {
        "device_id":device_id,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "temperature": round(random.uniform(20.0, 85.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "status": "CRITICAL" if random.random() < 0.05 else "OK",
        "is_late_simulated": is_late,
    }
    return device_id,event

def main():
    # 1. load config
    config = load_config()
    kafka_config = config["kafka"]
    gen_config = config["generator"]
    
    # Ưu tiên lấy biến môi trường từ Docker, nếu không có thì dùng file YAML
    # Lấy địa chỉ từ biến môi trường của Docker, nếu không có thì fallback về file YAML
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka_config["bootstrap_servers"])
    
    print(f"Khởi tạo Kafka Producer kết nối với {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer= lambda v: json.dumps(v).encode("utf-8")
    )
    """print("Khởi tạo Kafka Producer kết nối với Redpanda...")
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer= lambda v: json.dumps(v).encode("utf-8")
        )"""

    topic = kafka_config["topic_name"]
    rate_sleep = 1.0/gen_config["rate_per_sec"]

    print(f">>> Bắt đầu chuyền dữ liệu vào {topic}")
    print(f">>> Cấu hình: {gen_config['rate_per_sec']} msg/s | {int(gen_config['late_data_percent']*100)}% Late Data")
    print(">>> Nhẫn Ctrl + C để dừng. \n")

    total_sent = 0
    late_count = 0

    try:
        while True:
            device_id, event = generator_sensor_event(
                gen_config["devices"],
                gen_config["late_data_percent"],
                gen_config["min_delay_sec"],
                gen_config["max_delay_sec"]
            )

            # Gửi tín hiệu vào Kafka (device_id làm Partition key)
            producer.send(topic,key=device_id,value=event)

            total_sent += 1
            if event["is_late_simulated"]:
                late_count += 1

            if total_sent % 20 ==0:
                late_ratio_actual = (late_count/total_sent)*100
                print(f"[SENT {total_sent} msgs] Đã phát {total_sent} sự kiện | " f"Tỷ lệ dữ liệu trễ thực tế: {late_ratio_actual:.1f}%")

            time.sleep(rate_sleep)
            
    except KeyboardInterrupt:
        print("\n>>> Đã dừng Generator.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()