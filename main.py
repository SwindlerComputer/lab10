# Lab 10 – Protocol Buffers over MQTT
# Nelson Ude (C20479276) | Mateusz Matijuk (C21473436) | Luca Ursache (C21392706)

import network, time, machine
from umqtt.simple import MQTTClient
import temperature_upb2 as pb   # uprotobuf-generated file



BROKER_IP  = "172.20.10.8"       # Raspberry Pi MQTT Broker IP
TOPIC      = b"temp/pico_pb"     # Topic name
OUTPUT_PIN = None                # Publisher → "LED"; Subscriber → None
PUB_IDENT  = 1                   # Subscriber → any ID; Publisher → None

# ===== Wi-Fi credentials =====
SSID = "Emeka Ude"
PASS = "Internet2002"


# ------------------------------------------------------------
# Wi-Fi
# ------------------------------------------------------------
def connect_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(SSID, PASS)

    while not wlan.isconnected():
        print("Connecting to Wi-Fi...")
        time.sleep(1)

    print("Connected:", wlan.ifconfig())



def read_temp():
    adc = machine.ADC(4)
    conv = 3.3 / 65535
    val = adc.read_u16() * conv
    temp = 27 - (val - 0.706) / 0.001721
    return round(temp, 2)


def now_time():
    t = time.localtime()
    return (t[3], t[4], t[5])



def get_role():
    """
    Publisher → OUTPUT_PIN set, PUB_IDENT None
    Subscriber → PUB_IDENT set, OUTPUT_PIN None
    Anything else → fail gracefully
    """
    if OUTPUT_PIN is not None and PUB_IDENT is None:
        return "publisher"

    if PUB_IDENT is not None and OUTPUT_PIN is None:
        return "subscriber"

    return None


# ------------------------------------------------------------
# MQTT
# ------------------------------------------------------------
def connect_mqtt(client_id):
    client = MQTTClient(client_id, BROKER_IP)
    client.connect()
    return client



def build_message(pub_id, temp, hms):
    # Build nested Time message
    tmsg = pb.TimeMessage()
    tmsg.hour.setValue(hms[0])
    tmsg.minute.setValue(hms[1])
    tmsg.second.setValue(hms[2])

    # Must serialize nested message to bytes
    t_bytes = tmsg.serialize()

    # Build main Reading message
    rmsg = pb.ReadingMessage()
    rmsg.temperature.setValue(temp)
    rmsg.published_id.setValue(pub_id)
    rmsg.time.setValue(t_bytes)

    return rmsg


def parse_message(data):
    rmsg = pb.ReadingMessage()
    rmsg.parse(data)

    temp = float(rmsg.temperature.value())
    pid  = int(rmsg.published_id.value())

    tmsg = rmsg.time.value()
    h = int(tmsg.hour.value())
    m = int(tmsg.minute.value())
    s = int(tmsg.second.value())

    return pid, temp, (h, m, s)


# ------------------------------------------------------------
# Publisher
# ------------------------------------------------------------
def run_publisher():
    led = machine.Pin("LED", machine.Pin.OUT)
    client = connect_mqtt("publisher")

    while True:
        temp = read_temp()
        hms = now_time()

        msg = build_message(pub_id=1, temp=temp, hms=hms)
        raw = msg.serialize()

        client.publish(TOPIC, raw)
        print("Published:", temp, hms)

        led.value(1)
        time.sleep(0.1)
        led.value(0)
        time.sleep(2)


# ------------------------------------------------------------
# Subscriber
# ------------------------------------------------------------
def run_subscriber():
    led = machine.Pin("LED", machine.Pin.OUT)
    client = connect_mqtt("subscriber")

    latest = {}       # {publisher_id: (temp, timestamp)}
    LIMIT = 600       # 10 minutes
    THRESH = 25.0     # LED threshold

    def purge():
        now = time.time()
        for k in list(latest.keys()):
            if now - latest[k][1] > LIMIT:
                del latest[k]

    def avg_temp():
        if not latest:
            return None
        return sum(v[0] for v in latest.values()) / len(latest)

    def on_message(topic, payload):
        try:
            pid, temp, hms = parse_message(payload)
        except Exception as e:
            print("Failed to parse protobuf:", e)
            return

        latest[pid] = (temp, time.time())
        purge()

        avg = avg_temp()
        print("Received:", pid, temp, hms, "avg:", avg)

        if avg is not None and avg >= THRESH:
            led.value(1)
        else:
            led.value(0)

    client.set_callback(on_message)
    client.subscribe(TOPIC)
    print("Subscriber active...")

    while True:
        client.check_msg()
        time.sleep(0.2)


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    connect_wifi()
    role = get_role()

    if role == "publisher":
        run_publisher()
    elif role == "subscriber":
        run_subscriber()
    else:
        print("Invalid configuration. Program ending safely.")
        return


main()
