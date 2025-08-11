import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'turbine-data'

NUM_TURBINES = 10

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Produtor Kafka conectado com sucesso.")
        return producer
    except Exception as e:
        print(f"Erro ao conectar com o Kafka: {e}")
        return None

def generate_sensor_data(turbine_id):
    """Gera dados simulados para uma única turbina eólica."""
    return {
        'turbine_id': f'T{turbine_id:03d}',
        'timestamp': datetime.now().isoformat(),
        'wind_speed': round(random.uniform(0, 100), 2),
        'rpm': round(random.uniform(0, 20), 2),
        'power_output': round(random.uniform(0, 2.5), 4), 
        'vibration': round(random.uniform(0, 1.5), 4),
        'temperature': round(random.uniform(-10, 40), 2) 
    }

def main():
    producer = create_producer()
    if not producer:
        return

    try:
        while True:
            for i in range(1, NUM_TURBINES + 1):
                data = generate_sensor_data(i)
                print(f"Enviando dados: {data}")
                future = producer.send(KAFKA_TOPIC, value=data)
                try:
                    record_metadata = future.get(timeout=10)
                    print(f"Enviado com sucesso: tópico={record_metadata.topic}, partição={record_metadata.partition}")
                except Exception as e:
                    print(f"Falha ao enviar para Kafka: {e}")

            
            producer.flush()
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nSimulação interrompida pelo usuário.")
    finally:
        if producer:
            producer.close()
            print("Produtor Kafka desconectado.")

if __name__ == "__main__":
    main()
