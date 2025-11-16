from kafka import KafkaProducer
import csv
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_path = '/Users/liuyingjie/flink_workspace/UserBehavior.csv'

with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)  # ✅ 自动读取表头
    for row in reader:
        # 类型转换
        row['user_id'] = int(row['user_id'])
        row['item_id'] = int(row['item_id'])
        row['category_id'] = int(row['category_id'])
        # 秒级时间戳转毫秒级整数
        row['ts_ms'] = int(row['ts_ms']) * 1000

        producer.send('user_behavior', value=row)
        print(f"Sent: {row}")
        time.sleep(5)

producer.flush()
producer.close()
