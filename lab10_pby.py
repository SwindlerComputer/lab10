# lab10_pb.py
# Lab 10 – Protocol Buffers over MQTT
# Nelson Ude (C20479276) | Mateusz Matijuk (C21473436) | Luca Ursache (C21392706)

import network, time, machine
from umqtt.simple import MQTTClient
import temperature_upb2 as temperature  # uprotobuf-generated module




BROKER_IP  = "172.20.10.8"    
TOPIC      = b"temp/pico_pb"  # topic for MQTT
OUTPUT_PIN = 'LED'            # Publisher: "LED"; Subscriber: None
PUB_IDENT  = None              # Subscriber: non-None; Publisher: None

# ===== Wi-Fi credentials =====
SSID = "Emeka Ude"
PASS = "Internet2002"


def connect_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(SSID, PASS)
    while not wlan.isconnected():
        print("Connecting to Wi-Fi...")
        time.sleep(1)
    print("Connected:", wlan.ifconfig())


def read_temp():
    """Read internal temperature and return °C."""
    adc = machine.ADC(4)
    conv = 3.3 / 65535
    val = adc.read_u16() * conv
    temp_c = 27 - (val - 0.706) / 0.001721
    return round(temp_c, 2)


def now_time():
    """Return (hour, minute, second)."""
    t = time.localtime()
    return (t[3], t[4], t[5])


# ===== Role detection =====
def get_role():
    """
    Publisher: BROKER_IP, TOPIC, OUTPUT_PIN set; PUB_IDENT is None
    Subscriber: BROKER_IP, TOPIC, PUB_IDENT set; OUTPUT_PIN is None
    Otherwise: invalid configuration.
    """
    if BROKER_IP and TOPIC and OUTPUT_PIN and PUB_IDENT is None:
        return "publisher"
    elif BROKER_IP and TOPIC and PUB_IDENT is not None and OUTPUT_PIN is None:
        return "subscriber"
    else:
        print("Invalid configuration for lab10_pb.py")
        print("Publisher  => OUTPUT_PIN='LED', PUB_IDENT=None")
        print("Subscriber => OUTPUT_PIN=None, PUB_IDENT=1 (or any non-None)")
        return None


# ===== MQTT connect =====
def connect_mqtt(client_id: str) -> MQTTClient:
    client = MQTTClient(client_id, BROKER_IP)
    client.connect()
    return client



def build_reading(pub_id: int, temp: float, hms):
    """
    Build a Reading message with nested Time using uprotobuf.
    """
    # Build nested Time message (TimeMessage)
    tmsg = temperature.TimeMessage()
    tmsg.hour.setValue(int(hms[0]))
    tmsg.minute.setValue(int(hms[1]))
    tmsg.second.setValue(int(hms[2]))

    # Length/Message field expects raw bytes of the nested message
    time_bytes = tmsg.serialize()

    # Build Reading message (ReadingMessage)
    rmsg = temperature.ReadingMessage()
    rmsg.temperature.setValue(float(temp))
    rmsg.published_id.setValue(int(pub_id))
    rmsg.time.setValue(time_bytes)

    return rmsg


def parse_reading(data: bytes):
    """
    Parse bytes into ReadingMessage and unpack temperature, publisher_id, and time tuple.
    """
    rmsg = temperature.ReadingMessage()
    rmsg.parse(data)

    temp = float(rmsg.temperature.value())
    pid  = int(rmsg.published_id.value())

    # Nested time: rmsg.time.value() is a TimeMessage instance
    tmsg = rmsg.time.value()
    h = int(tmsg.hour.value())
    m = int(tmsg.minute.value())
    s = int(tmsg.second.value())

    return pid, temp, (h, m, s)


# ===== Publisher logic =====
def run_publisher():
    led = machine.Pin(OUTPUT_PIN, machine.Pin.OUT)
    client = connect_mqtt("pico_publisher")

    while True:
        temp_c = read_temp()
        hms = now_time()

        msg = build_reading(pub_id=1, temp=temp_c, hms=hms)
        raw = msg.serialize()

        client.publish(TOPIC, raw)
        print("Published:", temp_c, hms)

        led.value(1)
        time.sleep(0.1)
        led.value(0)
        time.sleep(2)


# ===== Subscriber logic =====
def run_subscriber():
    led = machine.Pin("LED", machine.Pin.OUT)
    client = connect_mqtt("pico_subscriber")

    latest = {}       # {publisher_id: (temp, timestamp)}
    LIMIT = 600       # 10 minutes in seconds
    THRESH = 25.0     

    def purge():
        now = time.time()
        for pid in list(latest.keys()):
            if now - latest[pid][1] > LIMIT:
                del latest[pid]

    def avg_temp():
        if not latest:
            return None
        return sum(v[0] for v in latest.values()) / len(latest)

    def on_message(topic, payload):
        try:
            pid, temp, hms = parse_reading(payload)
        except Exception as e:
            print("Failed to parse protobuf:", e)
            return

        latest[pid] = (temp, time.time())
        purge()
        avg = avg_temp()

        print("Received from", pid, "temp:", temp, "time:", hms, "avg:", avg)

        if avg is not None and avg >= THRESH:
            led.value(1)
        else:
            led.value(0)

    client.set_callback(on_message)
    client.subscribe(TOPIC)
    print("Subscribed to:", TOPIC)

    while True:
        client.check_msg()
        time.sleep(0.2)


# ===== Main =====
def main():
    connect_wifi()
    role = get_role()
    if role == "publisher":
        run_publisher()
    elif role == "subscriber":
        run_subscriber()
    else:
        # invalid config: fail gracefully
        pass


main()
