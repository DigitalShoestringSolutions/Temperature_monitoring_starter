#from paho.mqtt import client as mqtt_client
import paho.mqtt.client as mqtt_client
import time
import json

broker = '172.18.0.4'
port = 1883

client_id = f'wilsonscountry'

def connect_mqtt(client_id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect MQTT Broker, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
   # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client,msg_str, topic):
     msg_count = 0
     ##is this while needed

     data_out = json.dumps(msg_str)
     


    # while True:
    # time.sleep(1)
    # msg = f"messages: {msg_count}"
     result = client.publish(topic, data_out)
         # result: [0, 1]
     status = result[0]
     current_time = time.strftime("%H:%M:%S", time.localtime())
     print(current_time)
     if status == 0:
         print(f"Send `{data_out}` to topic `{topic}`")
     else:
         print(f"Failed to send message to topic {topic}")
         msg_count += 1


def run():
    client = connect_mqtt(client_id)
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()

