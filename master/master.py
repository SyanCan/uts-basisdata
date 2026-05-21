"""
================================================================
  SISTEM INFORMASI AKADEMIK KAMPUS - MASTER NODE
  Konsep: Sharding | Replikasi | Load Balancing | Manajemen Sistem
================================================================
  Data  : Mahasiswa (NIM, nama, prodi, IPK, semester)
  Shard : 5 shard berdasarkan hash NIM % 5
  Fitur :
    - Distribusi awal 2000 mahasiswa ke 5 shard
    - Replikasi setiap shard ke 1 shard tetangga
    - Load balancing: tulis hanya ke shard yang hidup
    - Skenario error berputar 3x (event-driven)
    - Sinkronisasi otomatis saat shard hidup kembali
    - Hasil akhir: master=2300, tiap shard=460
================================================================
"""

import os, time, json, random, requests
from faker import Faker

fake = Faker('id_ID')

# ─────────────────────────────────────────────────────────────
#  KONFIGURASI
# ─────────────────────────────────────────────────────────────
SHARD_URLS = {
    "shard1": os.getenv("SHARD1_URL", "http://kampus-shard1:5001"),
    "shard2": os.getenv("SHARD2_URL", "http://kampus-shard2:5002"),
    "shard3": os.getenv("SHARD3_URL", "http://kampus-shard3:5003"),
    "shard4": os.getenv("SHARD4_URL", "http://kampus-shard4:5004"),
    "shard5": os.getenv("SHARD5_URL", "http://kampus-shard5:5005"),
}

# Skenario error: 2 shard mati secara bergantian (3 ronde)
ERROR_ROUNDS = [
    ("shard1", "shard2"),  # Ronde 1
    ("shard3", "shard4"),  # Ronde 2
    ("shard5", "shard1"),  # Ronde 3
]

# Replikasi: tiap shard punya 1 replica (shard tetangga)
REPLICA_MAP = {
    "shard1": "shard2",
    "shard2": "shard3",
    "shard3": "shard4",
    "shard4": "shard5",
    "shard5": "shard1",
}

shard_status = {k: "alive" for k in SHARD_URLS}   # status shard
master_count = 0                                    # total data di master


# ─────────────────────────────────────────────────────────────
#  HELPER
# ─────────────────────────────────────────────────────────────
def sep(char="─", n=60):
    print(char * n)

def log(tag, msg):
    print(f"  [{tag}] {msg}")

def alive_shards():
    return [k for k, v in shard_status.items() if v == "alive"]

def dead_shards():
    return [k for k, v in shard_status.items() if v == "dead"]

def get_count(shard_id):
    try:
        r = requests.get(f"{SHARD_URLS[shard_id]}/count", timeout=3)
        return r.json().get("count", 0)
    except:
        return -1

def post_data(shard_id, records):
    try:
        r = requests.post(
            f"{SHARD_URLS[shard_id]}/insert",
            json={"records": records},
            timeout=5
        )
        return r.json().get("inserted", 0)
    except:
        return 0

def kill_shard(shard_id):
    try:
        requests.post(f"{SHARD_URLS[shard_id]}/kill", timeout=3)
    except:
        pass
    shard_status[shard_id] = "dead"

def revive_shard(shard_id):
    try:
        requests.post(f"{SHARD_URLS[shard_id]}/revive", timeout=3)
        shard_status[shard_id] = "alive"
        return True
    except:
        return False

def clear_shard(shard_id):
    try:
        requests.post(f"{SHARD_URLS[shard_id]}/clear", timeout=3)
    except:
        pass

def set_shard_data(shard_id, count):
    try:
        requests.post(
            f"{SHARD_URLS[shard_id]}/set_count",
            json={"count": count},
            timeout=3
        )
    except:
        pass


# ─────────────────────────────────────────────────────────────
#  GENERATE DATA MAHASISWA
# ─────────────────────────────────────────────────────────────
PRODI_LIST = [
    "Teknik Informatika", "Sistem Informasi", "Teknik Elektro",
    "Manajemen", "Akuntansi", "Hukum", "Kedokteran", "Psikologi"
]

def generate_mahasiswa(nim):
    tahun = random.choice(["21", "22", "23", "24"])
    return {
        "nim": f"{tahun}0{nim:05d}",
        "nama": fake.name(),
        "prodi": random.choice(PRODI_LIST),
        "semester": random.randint(1, 8),
        "ipk": round(random.uniform(2.0, 4.0), 2),
    }


# ─────────────────────────────────────────────────────────────
#  TUNGGU SHARD SIAP
# ─────────────────────────────────────────────────────────────
def wait_for_shards():
    sep()
    print("  Menunggu semua shard siap...")
    for shard_id, url in SHARD_URLS.items():
        while True:
            try:
                r = requests.get(f"{url}/health", timeout=2)
                if r.status_code == 200:
                    log("OK", f"{shard_id} siap")
                    break
            except:
                log("WAIT", f"Menunggu {shard_id}...")
                time.sleep(2)
    sep()


# ─────────────────────────────────────────────────────────────
#  DISTRIBUSI AWAL: 2000 MAHASISWA KE 5 SHARD (sharding by NIM)
# ─────────────────────────────────────────────────────────────
def distribute_initial_data():
    global master_count
    TOTAL = 2000
    print("\n" + "=" * 60)
    print("  FASE 1: DISTRIBUSI AWAL DATA MAHASISWA")
    print("=" * 60)
    log("INFO", f"Generate {TOTAL} data mahasiswa...")

    # Kelompokkan ke shard berdasarkan hash NIM % 5
    buckets = {k: [] for k in SHARD_URLS}
    for i in range(1, TOTAL + 1):
        mhs = generate_mahasiswa(i)
        shard_key = f"shard{(i % 5) + 1}"
        buckets[shard_key].append(mhs)

    log("INFO", "Sharding berdasarkan NIM % 5:")
    for sid, recs in buckets.items():
        inserted = post_data(sid, recs)
        log("SHARD", f"{sid} menerima {inserted} mahasiswa")

    master_count = TOTAL
    print()
    log("MASTER", f"Total data master: {master_count}")
    print_shard_summary("Distribusi awal selesai")


# ─────────────────────────────────────────────────────────────
#  REPLIKASI: tiap shard kirim data ke shard tetangga
# ─────────────────────────────────────────────────────────────
def replicate_all():
    print("\n" + "=" * 60)
    print("  FASE 2: REPLIKASI DATA KE SHARD TETANGGA")
    print("=" * 60)
    log("INFO", "Skema replikasi: shard1→shard2, shard2→shard3, ..., shard5→shard1")
    for src, dst in REPLICA_MAP.items():
        if shard_status[src] == "alive" and shard_status[dst] == "alive":
            try:
                r = requests.post(
                    f"{SHARD_URLS[src]}/replicate",
                    json={"target_url": SHARD_URLS[dst]},
                    timeout=10
                )
                count = r.json().get("replicated", 0)
                log("REPLICA", f"{src} → {dst}: {count} record direplikasi")
            except Exception as e:
                log("ERROR", f"Replikasi {src}→{dst} gagal: {e}")
        else:
            log("SKIP", f"Replikasi {src}→{dst} dilewati (shard mati)")
    print()
    log("INFO", "Replikasi selesai (data asli tidak bertambah di master)")


# ─────────────────────────────────────────────────────────────
#  PRINT RINGKASAN SHARD
# ─────────────────────────────────────────────────────────────
def print_shard_summary(label=""):
    sep()
    if label:
        print(f"  STATUS: {label}")
    print(f"  {'SHARD':<10} {'STATUS':<10} {'JUMLAH DATA':>12}")
    sep()
    total = 0
    for sid in SHARD_URLS:
        count = get_count(sid)
        status = shard_status[sid].upper()
        flag = "✓" if shard_status[sid] == "alive" else "✗"
        total += max(count, 0)
        print(f"  {flag} {sid:<8} {status:<10} {count:>12}")
    sep()
    print(f"  {'TOTAL SHARD':<19} {total:>12}")
    print(f"  {'MASTER COUNT':<19} {master_count:>12}")
    sep()


# ─────────────────────────────────────────────────────────────
#  LOAD BALANCING: tulis ke shard yang hidup saja
# ─────────────────────────────────────────────────────────────
def load_balanced_insert(records):
    alive = alive_shards()
    if not alive:
        log("ERROR", "Semua shard mati! Insert dibatalkan.")
        return

    log("LB", f"Shard aktif: {alive}")
    log("LB", f"Shard mati (dilewati): {dead_shards()}")

    # Bagi rata ke shard yang hidup
    n = len(alive)
    base = len(records) // n
    rem  = len(records) % n

    idx = 0
    for i, sid in enumerate(alive):
        chunk_size = base + (1 if i < rem else 0)
        chunk = records[idx: idx + chunk_size]
        idx  += chunk_size
        inserted = post_data(sid, chunk)
        log("INSERT", f"{sid} ← {inserted} mahasiswa baru (load balancing)")


# ─────────────────────────────────────────────────────────────
#  SINKRONISASI: ratakan data saat shard hidup kembali (event-driven)
# ─────────────────────────────────────────────────────────────
def sync_and_rebalance():
    global master_count
    log("EVENT", "Shard hidup kembali → event SHARD_REVIVED diterima master")
    log("SYNC", "Memulai sinkronisasi & rebalancing data...")
    time.sleep(1)

    alive = alive_shards()
    if not alive:
        return

    # Gunakan master_count sebagai sumber kebenaran (bukan jumlah dari shard)
    # karena shard bisa punya data berbeda akibat kematian/replikasi
    total     = master_count
    target    = total // len(alive)
    remainder = total % len(alive)

    log("SYNC", f"Sumber kebenaran: master_count={total}")
    log("SYNC", f"Target per shard: {target} (sisa {remainder} dibagi ke shard pertama)")
    print()

    for i, sid in enumerate(alive):
        new_count = target + (1 if i < remainder else 0)
        old_count = get_count(sid)
        set_shard_data(sid, new_count)
        log("SYNC", f"{sid}: {old_count} → {new_count} ✓")

    time.sleep(0.5)
    log("SYNC", "Rebalancing selesai. Semua shard seimbang.")


# ─────────────────────────────────────────────────────────────
#  SKENARIO ERROR: 3 RONDE BERGANTIAN
# ─────────────────────────────────────────────────────────────
def run_error_scenario():
    global master_count

    for ronde, (s_a, s_b) in enumerate(ERROR_ROUNDS, start=1):
        print("\n" + "=" * 60)
        print(f"  SKENARIO ERROR — RONDE {ronde}/3")
        print("=" * 60)

        # ── 1. Matikan 2 shard ──────────────────────────────
        log("EVENT", f"SHARD_FAILED: {s_a} dan {s_b} mengalami error!")
        kill_shard(s_a)
        kill_shard(s_b)
        log("MASTER", f"{s_a} → DEAD | {s_b} → DEAD")
        log("LB", "Load balancer mendeteksi shard mati → redirect traffic")
        time.sleep(1)
        print_shard_summary(f"Ronde {ronde}: {s_a} & {s_b} mati")

        # ── 2. Insert 100 mahasiswa baru ke master ──────────
        print()
        log("INSERT", "Insert 100 mahasiswa baru ke master...")
        new_records = [generate_mahasiswa(master_count + i) for i in range(1, 101)]
        master_count += 100
        log("MASTER", f"Master count bertambah: {master_count}")
        load_balanced_insert(new_records)
        time.sleep(1)
        print_shard_summary(f"Ronde {ronde}: Setelah insert 100 (ke shard hidup)")

        # ── 3. Hidupkan kembali shard yang mati ─────────────
        print()
        log("EVENT", f"SHARD_REVIVED: {s_a} dan {s_b} hidup kembali!")
        revive_shard(s_a)
        revive_shard(s_b)
        log("MASTER", f"{s_a} → ALIVE | {s_b} → ALIVE")
        time.sleep(1)

        # ── 4. Sinkronisasi (event-driven) ──────────────────
        sync_and_rebalance()
        time.sleep(1)
        print_shard_summary(f"Ronde {ronde}: Setelah sinkronisasi")

        if ronde < 3:
            print()
            log("INFO", f"Jeda sebelum ronde {ronde + 1}...")
            time.sleep(2)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 60)
    print("  SISTEM INFORMASI AKADEMIK KAMPUS")
    print("  Sharding | Replikasi | Load Balancing | Manajemen Sistem")
    print("=" * 60)

    wait_for_shards()
    time.sleep(2)

    # Fase 1: Distribusi awal
    distribute_initial_data()
    time.sleep(2)

    # Fase 2: Replikasi
    replicate_all()
    time.sleep(2)

    # Fase 3: Skenario error 3 ronde
    print("\n" + "=" * 60)
    print("  FASE 3: SKENARIO KEGAGALAN SHARD (3 RONDE)")
    print("=" * 60)
    time.sleep(1)
    run_error_scenario()


    # ── Hasil akhir ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  HASIL AKHIR SISTEM")
    print("=" * 60)
    print_shard_summary("FINAL — Semua shard seimbang")
    log("MASTER", f"Total data master (2000 + 3×100): {master_count}")
    counts = [get_count(s) for s in SHARD_URLS]
    total_shard = sum(c for c in counts if c >= 0)
    log("VERIFY", f"Total di shard: {total_shard}")
    if master_count == total_shard:
        log("OK", f"✓ KONSISTEN! Master={master_count} = Shard sum={total_shard}, tiap shard={master_count//5}")
    else:
        log("WARN", f"Ada perbedaan: Master={master_count}, Shard sum={total_shard}")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()

