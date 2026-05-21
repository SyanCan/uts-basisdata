"""
================================================================
  SISTEM INFORMASI AKADEMIK KAMPUS - SHARD NODE
  Setiap shard adalah server Flask yang menerima perintah
  dari master: insert, replicate, kill, revive, sync
================================================================
"""

import os, json, requests
from flask import Flask, request, jsonify

app = Flask(__name__)

SHARD_ID   = os.getenv("SHARD_ID",   "shard_unknown")
SHARD_PORT = int(os.getenv("SHARD_PORT", 5001))

# Storage in-memory (simulasi database lokal shard)
storage         = []   # data ASLI milik shard ini
replica_storage = []   # data REPLICA dari shard lain (tidak dihitung di /count)
is_alive        = True # status shard (hidup/mati)


# ─────────────────────────────────────────────────────────────
#  HEALTH CHECK
# ─────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"shard": SHARD_ID, "status": "alive" if is_alive else "dead"})


# ─────────────────────────────────────────────────────────────
#  JUMLAH DATA
# ─────────────────────────────────────────────────────────────
@app.route("/count", methods=["GET"])
def count():
    if not is_alive:
        return jsonify({"error": "shard mati"}), 503
    # Hanya hitung data ASLI, bukan replica
    return jsonify({"shard": SHARD_ID, "count": len(storage)})


# ─────────────────────────────────────────────────────────────
#  INSERT DATA MAHASISWA
# ─────────────────────────────────────────────────────────────
@app.route("/insert", methods=["POST"])
def insert():
    global storage
    if not is_alive:
        return jsonify({"error": "shard mati, tidak bisa insert"}), 503

    records = request.json.get("records", [])
    storage.extend(records)
    return jsonify({
        "shard": SHARD_ID,
        "inserted": len(records),
        "total": len(storage)
    })


# ─────────────────────────────────────────────────────────────
#  REPLIKASI: kirim semua data ke shard tetangga
# ─────────────────────────────────────────────────────────────
@app.route("/replicate", methods=["POST"])
def replicate():
    if not is_alive:
        return jsonify({"error": "shard mati"}), 503

    target_url = request.json.get("target_url")
    if not target_url:
        return jsonify({"error": "target_url diperlukan"}), 400

    try:
        r = requests.post(
            f"{target_url}/receive_replica",
            json={"source": SHARD_ID, "records": storage},
            timeout=10
        )
        return jsonify({"replicated": len(storage), "target": target_url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────
#  TERIMA REPLIKASI DARI SHARD LAIN
# ─────────────────────────────────────────────────────────────
@app.route("/receive_replica", methods=["POST"])
def receive_replica():
    global replica_storage
    if not is_alive:
        return jsonify({"error": "shard mati"}), 503

    source  = request.json.get("source", "unknown")
    records = request.json.get("records", [])

    # Simpan di replica_storage (TERPISAH dari data asli)
    replica_records = [{**r, "_replica_from": source} for r in records]
    replica_storage.extend(replica_records)

    return jsonify({
        "shard":         SHARD_ID,
        "received_from": source,
        "replica_count": len(records),
        "real_data":     len(storage),
        "replica_data":  len(replica_storage)
    })


# ─────────────────────────────────────────────────────────────
#  MATIKAN SHARD (simulasi error)
# ─────────────────────────────────────────────────────────────
@app.route("/kill", methods=["POST"])
def kill():
    global is_alive
    is_alive = False
    print(f"[{SHARD_ID}] ⚠ SHARD DIMATIKAN oleh master (simulasi error)")
    return jsonify({"shard": SHARD_ID, "status": "dead"})


# ─────────────────────────────────────────────────────────────
#  HIDUPKAN KEMBALI SHARD
# ─────────────────────────────────────────────────────────────
@app.route("/revive", methods=["POST"])
def revive():
    global is_alive
    is_alive = True
    print(f"[{SHARD_ID}] ✓ SHARD DIHIDUPKAN KEMBALI oleh master")
    return jsonify({"shard": SHARD_ID, "status": "alive"})


# ─────────────────────────────────────────────────────────────
#  HAPUS SEMUA DATA (untuk reset)
# ─────────────────────────────────────────────────────────────
@app.route("/clear", methods=["POST"])
def clear():
    global storage, replica_storage
    storage         = []
    replica_storage = []
    return jsonify({"shard": SHARD_ID, "status": "cleared"})


# ─────────────────────────────────────────────────────────────
#  SET COUNT LANGSUNG (untuk sinkronisasi dari master)
#  Master menghitung target per shard, lalu set ke sini
# ─────────────────────────────────────────────────────────────
@app.route("/set_count", methods=["POST"])
def set_count():
    global storage
    if not is_alive:
        return jsonify({"error": "shard mati"}), 503

    new_count = request.json.get("count", 0)
    current   = len(storage)

    if new_count > current:
        # Tambahkan placeholder record (simulasi sync dari master)
        diff = new_count - current
        for i in range(diff):
            storage.append({"_synced": True, "id": current + i})
    elif new_count < current:
        # Potong data berlebih
        storage = storage[:new_count]

    print(f"[{SHARD_ID}] SYNC: {current} → {len(storage)}")
    return jsonify({
        "shard": SHARD_ID,
        "before": current,
        "after": len(storage)
    })


# ─────────────────────────────────────────────────────────────
#  GET SEMUA DATA (opsional, untuk debugging)
# ─────────────────────────────────────────────────────────────
@app.route("/data", methods=["GET"])
def get_data():
    if not is_alive:
        return jsonify({"error": "shard mati"}), 503
    return jsonify({
        "shard":        SHARD_ID,
        "real_count":   len(storage),
        "replica_count": len(replica_storage),
        "sample":       storage[:5]
    })


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[{SHARD_ID}] Shard node berjalan di port {SHARD_PORT}")
    app.run(host="0.0.0.0", port=SHARD_PORT)
