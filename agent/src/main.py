from paho.mqtt import client as mqtt_client
import json
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.parking_schema import ParkingSchema
from file_datasource import FileDatasource
import config


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, topic, topic2, datasource, delay):
    input_array = datasource.read_csv_file1_to_array()
    input_array2 = datasource.read_csv_file2_to_array()
    input_array3 = datasource.read_csv_file3_to_array()

    data_thread, output_array, output_array2 =datasource.startReading(input_array, input_array2, input_array3)

    while True:
        time.sleep(delay)
        while len(output_array) <= 0:
                time.sleep(1)
        data = datasource.read(output_array)
        data_par = datasource.read_par(output_array2)
        msg = AggregatedDataSchema().dumps(data)
        msg_par = ParkingSchema().dumps(data_par)
        result = client.publish(topic, msg)
        result = client.publish(topic2, msg_par)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            pass
            # print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")#, None, None, None)
    # Infinity publish data
    publish(client, config.MQTT_TOPIC, config.MQTT_TOPIC_2, datasource, config.DELAY)


if __name__ == "__main__":
    run()