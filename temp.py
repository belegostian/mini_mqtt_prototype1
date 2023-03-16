import time
import paho.mqtt.client as mqtt
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.log import setLogLevel
from multiprocessing import Process


# Mininet topology
class TestTopology(Topo):
    "Simple topology example."

    def build(self):
        # Add hosts and switches
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')
        h3 = self.addHost('h3')

        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')

        # Add links
        self.addLink(h1, s1)
        self.addLink(h2, s1)

        self.addLink(h3, s2)

        self.addLink(s1, s2)


def run_topology():
    topo = TestTopology()
    net = Mininet(topo=topo)
    net.start()

    print("Testing network connectivity")
    net.pingAll()

    h1 = net.get('h1')
    h2 = net.get('h2')
    h3 = net.get('h3')

    mqtt_broker = h1.IP()

    publisher_process = Process(target=publisher, args=(mqtt_broker,))
    publisher_process.start()

    subscriber_process_h2 = Process(target=subscriber, args=(mqtt_broker,))
    subscriber_process_h2.start()

    subscriber_process_h3 = Process(target=subscriber, args=(mqtt_broker,))
    subscriber_process_h3.start()

    publisher_process.join()
    subscriber_process_h2.join()
    subscriber_process_h3.join()

    net.stop()


# MQTT publisher
def publisher(broker_ip):
    def on_connect(client, userdata, flags, rc):
        print(f"Publisher connected with result code {rc}")
        client.subscribe("data_freshness")

    client = mqtt.Client()
    client.on_connect = on_connect

    client.connect(broker_ip, 1883, 60)

    while True:
        client.publish("data_freshness", "Fresh Data")
        time.sleep(1)


# MQTT subscriber
def subscriber(broker_ip):
    def on_connect(client, userdata, flags, rc):
        print(f"Subscriber connected with result code {rc}")
        client.subscribe("data_freshness")

    def on_message(client, userdata, msg):
        print(f"{msg.topic}: {msg.payload.decode()}")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker_ip, 1883, 60)
    client.loop_forever()


if __name__ == '__main__':
    setLogLevel('info')
    run_topology()
