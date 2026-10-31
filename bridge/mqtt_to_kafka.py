# mqtt_to_kafka.py dan dokumentasinya
# Sistem Monitoring Reaktor Nuklir, PID C
# M Dzakir Thuha Al Ghaffar (245150307111004)
# Abdillah Abyan Ilman Nafian (245150300111003)
# Faaza Fauzan Adzim (245150307111010)
# Ivang Arfandia (245150307111003)
# Fathian Auzaie Hanzalah (245150307111007)

import os, json
from kafka import KafkaProducer
import paho.mqtt.client as mqtt

# Mengambil konfigurasi dari environment variable Docker
# Hal ini memungkinkan kita mengubah pengaturan lewat docker-compose.yml tanpa perlu menyentuh kode program ini
BOOTSTRAP   = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC_KAFKA = os.getenv("TOPIC_KAFKA", "iot.sensors")
BROKER_MQTT = os.getenv("BROKER_MQTT", "mosquitto")

# Topik subscription menggunakan wildcard '+'
# Pola 'reactor/+/metrics' berarti kode ini akan menerima data dari semua reaktor
# contoh reactor/RX-01/metrics, reactor/RX-02/metrics, dst. Tapi disini kita hanya ada satu reaktor
TOPIC_MQTT  = os.getenv("TOPIC_MQTT", "reactor/+/metrics") 

# Inisialisasi producer Kafka, kafka bekerja dengan byte, bukan string atau objek JSON.
# Oleh karena itu, kita mendefinisikan serializer untuk otomatis mengubah data JSON dan Key menjadi format byte atau utf-8 sebelum dikirim
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

# Callback yang dipanggil otomatis saat berhasil terhubung ke Broker MQTT
def on_connect(client, userdata, flags, rc):
    # Parameter flush=True memastikan log langsung muncul di terminal Docker, tidak di buffer
    print(f"Bridge Connected to MQTT ({rc}). Subscribing: {TOPIC_MQTT}", flush=True)
    # Mulai mendengarkan topik sesuai pola wildcard yang ditentukan
    client.subscribe(TOPIC_MQTT)

# Callback utama, dipanggil setiap kali ada data masuk dari sensor/simulator
# Fungsi ini melakukan tugas Extract sederhana dan Ingest ke Kafka
def on_message(client, userdata, msg):
    try:
        # Dekode payload dari byte MQTT menjadi string, lalu parse menjadi dictionary Python
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)
        
        # Partitioning Kafka, menggunakan 'reactor_id' sebagai Key saat mengirim ke Kafka
        # Ini menjamin bahwa semua data dari satu reaktor tertentu, misal RX-01 akan selalu masuk ke partisi Kafka yang sama secara berurutan
        # Jika key tidak diset atau none, data bisa tersebar acak dan urutan waktunya bisa kacau
        key = data.get("reactor_id", "unknown_reactor")
        
        # Mengirim data ke Kafka
        producer.send(TOPIC_KAFKA, key=key, value=data)
        
        # Log singkat untuk memantau nilai RRSI di terminal
        print(f"MQTT->Kafka [{key}]: RRSI={data.get('rrsi_score')}", flush=True)
        
    except Exception as e:
        # Menangkap error, misal JSON rusak agar service bridge tidak crash
        print(f"Error bridging message: {e}", flush=True)

# Setup klien MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Memulai koneksi ke Broker MQTT
print(f"Connecting to MQTT Broker: {BROKER_MQTT}...", flush=True)
client.connect(BROKER_MQTT, 1883, 60)

# Menjalankan loop pemrosesan jaringan secara terus menerus atau blocking
# Ini mencegah kode berhenti sehingga bridge terus berjalan
client.loop_forever()