# mqtt_publisher.py dan dokumentasinya
# Sistem Monitoring Reaktor Nuklir, PID C
# M Dzakir Thuha Al Ghaffar (245150307111004)
# Abdillah Abyan Ilman Nafian (245150300111003)
# Faaza Fauzan Adzim (245150307111010)
# Ivang Arfandia (245150307111003)
# Fathian Auzaie Hanzalah (245150307111007) 

import json
import time
import random
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# Konfigurasi koneksi MQTT ke broker Mosquitto
# Topik ini digunakan untuk publikasi data telemetri sensor
BROKER = "localhost"
PORT = 1883 # Menggunakan localhost dan port standar 1883
TOPIC = "reactor/room-01/metrics"

# Inisialisasi client MQTT dengan ID unik untuk menghindari konflik sesi
client = mqtt.Client("ReactorSimulator_Final")
try:
    client.connect(BROKER, PORT, 60)
    client.loop_start()
    print(f"[INIT] Terhubung ke Broker {BROKER}:{PORT}")
except Exception as e:
    print(f"[ERROR] Gagal konek ke MQTT: {e}")
    exit(1)

# Konfigurasi Sensor Limit dan Bobot sesuai Laporan Proyek Akhir, ini menyimpan parameter operasi untuk 12 sensor
# Limits berisi tiga nilai batas untuk Aman, Waspada dan Maintenance, nilai di luar batas ketiga secara otomatis dianggap Bahaya atau Danger
# Weight adalah bobot kontribusi sensor terhadap skor total RRSI serta total penjumlahan seluruh bobot adalah 1.0 atau 100 persen
SENSORS = {
    # Kategori Radiasi memiliki prioritas tinggi dan bobot besar
    "gamma":        {"limits": [0.5, 10, 25],    "weight": 0.30}, 
    "neutron":      {"limits": [0.5, 10, 25],    "weight": 0.20}, 
    "noble_gas":    {"limits": [50, 300, 1000],  "weight": 0.08}, 
    "alpha_beta":   {"limits": [5, 10, 20],      "weight": 0.04}, 
    
    # Kategori Lingkungan memiliki bobot menengah
    # Untuk sensor tipe inverse dimana nilai makin kecil makin bahaya, contohnya adalah tekanan filter dan aliran udara ventilasi
    "temperature":  {"limits": [28, 35, 45],     "weight": 0.08}, 
    "cool_pressure":{"limits": [5.5, 6.0, 6.5],  "weight": 0.08}, 
    "diff_pressure":{"limits": [80, 60, 40],     "weight": 0.06},
    "airflow":      {"limits": [90, 80, 60],     "weight": 0.06},
    "humidity":     {"limits": [60, 70, 80],     "weight": 0.04}, 
    
    # Kategori Kualitas Udara memiliki bobot Rendah
    "co2":          {"limits": [800, 1200, 2000],"weight": 0.02}, 
    "toxic_gas":    {"limits": [10, 20, 40],     "weight": 0.02}, 
    "particulate":  {"limits": [35, 75, 120],    "weight": 0.02}, 
}

# Fungsi Logika Status Kode 0 sampai 3
# Menentukan status sensor menjadi Aman Waspada Maintenance atau Bahaya
# Logika ini juga membedakan sensor tipe Normal dan Inverse secara otomatis
def get_status_code(key, val):
    lim = SENSORS[key]['limits']
    is_inverse = lim[0] > lim[2] 

    # Kode 0 adalah Aman, kode 1 adalah Waspada, kode 2 adalah Perbaikan atau Maintenance, kode 3 adalah Bahaya
    if is_inverse: 
        if val >= lim[0]: return 0
        if val >= lim[1]: return 1
        if val >= lim[2]: return 2
        return 3
    else: 
        if val <= lim[0]: return 0
        if val <= lim[1]: return 1
        if val <= lim[2]: return 2
        return 3

# Fungsi ini mengkalkulasi skor RRSI Normalisasi 0 sampai 100 dan mengubah nilai sensor mentah menjadi skor terstandarisasi dengan enggunakan rumus yang ada pada laporan
# Nilai skor dibatasi maksimal 100 dan minimal 0, skor yang tinggi menandakan kondisi yang semakin berbahaya
def calculate_score(key, val):
    lim = SENSORS[key]['limits']
    safe_limit, danger_limit = lim[0], lim[2]
    
    if safe_limit > danger_limit:
        score = ((safe_limit - val) / (safe_limit - danger_limit)) * 100
    else:
        score = ((val - safe_limit) / (danger_limit - safe_limit)) * 100
        
    return max(0, min(100, score))

# Generator Data Skenario Simulasi yang enghasilkan nilai dummy sensor yang dimanipulasi sesuai kondisi
# Bertujuan menguji respons sistem terhadap 5 kondisi operasi berbeda, skenario juga meliputi Aman Waspada Maintenance Bahaya dan Veto
def generate_values_by_scenario(scenario):
    data = {}
    
    # Menetapkan nilai dasar atau Baseline kondisi aman
    bases = {
        "gamma": 0.04, "neutron": 0.04, "noble_gas": 10, "alpha_beta": 1.0,
        "temperature": 24.0, "cool_pressure": 5.0, "diff_pressure": 100.0, "airflow": 98.0,
        "humidity": 45.0, "co2": 420, "toxic_gas": 0, "particulate": 15
    }

    # Memberikan variasi acak kecil agar data terlihat natural
    for k in bases:
        bases[k] += random.uniform(-bases[k]*0.02, bases[k]*0.02) if bases[k] > 0 else 0

    if scenario == "SAFE":
        # Skenario Aman dimana semua nilai normal
        # Target RRSI dibawah 20
        data = bases

    elif scenario == "WARNING":
        # Skenario Waspada menaikkan sensor Gamma Neutron dan Suhu
        # Target RRSI antara 20 sampai 50
        data = bases.copy()
        data["gamma"] = random.uniform(5, 9)
        data["neutron"] = random.uniform(5, 9)
        data["temperature"] = random.uniform(30, 34)
        data["airflow"] = random.uniform(82, 88)
        data["diff_pressure"] = random.uniform(65, 75)

    elif scenario == "MAINTENANCE":
        # Skenario Perbaikan menaikkan sensor bobot besar
        # Target RRSI antara 50 sampai 70
        data = bases.copy()
        data["gamma"] = random.uniform(15, 22)
        data["neutron"] = random.uniform(15, 22)
        data["diff_pressure"] = random.uniform(42, 55)
        data["cool_pressure"] = random.uniform(6.1, 6.4)
        data["temperature"] = random.uniform(38, 42)

    elif scenario == "DANGER":
        # Skenario Bahaya menaikkan sensor secara signifikan
        # Target RRSI diatas 70
        data = bases.copy()
        data["gamma"] = random.uniform(30, 50)
        data["neutron"] = random.uniform(30, 50)
        data["temperature"] = random.uniform(46, 55)
        data["airflow"] = random.uniform(30, 50)
        data["toxic_gas"] = random.uniform(50, 80)

    elif scenario == "VETO":
        # Skenario Khusus Veto yaitu semua sensor aman kecuali satu sensor kritis yaitu Neutron
        # Untuk menguji fitur Critical Override
        data = bases.copy()
        data["neutron"] = 40.0

    return data

# Program Utama Loop Simulasi yang mencetak informasi siklus simulasi 60 detik sebagai output informasi di terminal
print("=== SIMULATOR REAKTOR NUKLIR (ALL RRSI CONDITIONS) ===")
print("Siklus Simulasi Looping setiap 60 detik")
print("00-12s SAFE        RRSI di bawah 20")
print("12-24s WARNING     RRSI 20 sampai 50")
print("24-36s MAINTENANCE RRSI 50 sampai 70")
print("36-48s DANGER      RRSI di atas 70")
print("48-60s VETO        Single Sensor Critical")
print("-" * 60)

try:
    while True:
        # Mengambil detik saat ini untuk mengatur perpindahan mode
        sec = datetime.now().second
        
        # Logika State Machine berdasarkan waktu detik
        if 0 <= sec < 12: mode = "SAFE"
        elif 12 <= sec < 24: mode = "WARNING"
        elif 24 <= sec < 36: mode = "MAINTENANCE"
        elif 36 <= sec < 48: mode = "DANGER"
        else: mode = "VETO"

        # Generate data sensor sesuai mode yang aktif
        raw_values = generate_values_by_scenario(mode)
        
        rrsi_total = 0
        veto_active = False
        
        # Menyiapkan payload data JSON dengan timestamp UTC
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "reactor_id": "REAKTOR-01",
            "scenario": mode,
            "event_id": str(uuid.uuid4())
        }
        
        # Iterasi perhitungan untuk setiap sensor
        for key, val in raw_values.items():
            payload[key] = round(val, 3)
            
            # Hitung status per sensor
            status = get_status_code(key, val)
            payload[f"{key}_status"] = status
            
            # Cek kondisi veto jika status sensor adalah Bahaya atau kode 3
            if status == 3:
                veto_active = True
                
            # Akumulasi skor RRSI berdasarkan bobot masing masing parameter dan sensor
            score = calculate_score(key, val)
            weight = SENSORS[key]['weight']
            rrsi_total += score * weight

        # Menentukan nilai akhir RRSI, jika veto aktif nilai dipaksa menjadi 100
        final_rrsi = 100.0 if veto_active else round(rrsi_total, 2)
        
        # Klasifikasi status sistem global berdasarkan nilai akhir RRSI
        # Status Aman kode 0, status Waspada kode 1, status Perbaikan kode 2, status Bahaya kode 3
        if final_rrsi <= 20:   sys_stat, sys_code = "AMAN", 0
        elif final_rrsi <= 50: sys_stat, sys_code = "WASPADA", 1
        elif final_rrsi <= 70: sys_stat, sys_code = "PERBAIKAN", 2
        else:                  sys_stat, sys_code = "BAHAYA", 3
        
        # Override teks status jika Veto terjadi
        if veto_active:
            sys_stat = "BAHAYA (VETO)"
            sys_code = 3

        # Menyusun data lengkap ke payload JSON
        payload["rrsi_score"] = final_rrsi
        payload["system_status_text"] = sys_stat
        payload["system_status_code"] = sys_code

        # Mengirim payload ke Broker MQTT pada topik yang ditentukan
        client.publish(TOPIC, json.dumps(payload))
        
        # Visualisasi output terminal dengan warna ANSI
        # Hijau untuk Aman, kuning untuk Waspada, oranye untuk Perbaikan dan merah untuk Bahaya
        color = "\033[92m"
        if sys_code == 1: color = "\033[93m"
        elif sys_code == 2: color = "\033[38;5;208m"
        elif sys_code == 3: color = "\033[91m"
        reset = "\033[0m"
        
        print(f"[{sec:02d}s|{mode[:4]}] {color}Status: {sys_stat:<15} | RRSI: {final_rrsi:>5}%{reset}")
        
        # Jeda satu detik sebelum siklus berikutnya
        time.sleep(1.0)

except KeyboardInterrupt:
    print("\nSimulasi dihentikan.")
    client.loop_stop()
    client.disconnect()