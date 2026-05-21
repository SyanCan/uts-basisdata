# SISTEM INFORMASI AKADEMIK KAMPUS
## Distributed Database: Sharding, Replikasi, Load Balancing, Manajemen Sistem

---

## STRUKTUR FILE

```
uts-basisdata/
├── docker-compose.yml       ← orkestrasi semua container
├── master/
│   ├── Dockerfile
│   ├── master.py            ← logika utama: sharding, LB, sync, skenario error
│   └── requirements.txt
└── shard/
    ├── Dockerfile
    ├── shard.py             ← server Flask tiap shard (dipakai 5x)
    └── requirements.txt
```

---

## CARA MENJALANKAN

### 1. Pastikan Docker Desktop sudah berjalan

### 2. Buka CMD / Terminal, masuk ke folder proyek
```
cd uts-basisdata
```

### 3. Build dan jalankan semua container
```
docker-compose up --build
```

### 4. Lihat output simulasi di CMD
Sistem akan otomatis menjalankan seluruh skenario:
- Distribusi 2000 data mahasiswa ke 5 shard
- Replikasi ke shard tetangga
- 3 ronde kegagalan shard (berputar)
- Insert 100 data per ronde ke shard yang hidup
- Sinkronisasi saat shard hidup kembali
- Hasil akhir: master=2300, tiap shard=460

### 5. Untuk menghentikan
```
docker-compose down
```

---

## KONSEP YANG DITERAPKAN

| Konsep          | Implementasi                                                  |
|-----------------|---------------------------------------------------------------|
| Sharding        | Data mahasiswa dibagi ke 5 shard berdasarkan NIM % 5         |
| Replikasi       | Tiap shard mengirim datanya ke shard tetangga (chain)        |
| Load Balancing  | Insert hanya ke shard hidup, dibagi rata secara otomatis     |
| Manajemen Sistem| Master mengatur kill/revive shard + sinkronisasi data        |

## SKENARIO ERROR (3 RONDE)

| Ronde | Shard Mati | Insert ke     |
|-------|------------|---------------|
| 1     | shard1, 2  | shard3, 4, 5  |
| 2     | shard3, 4  | shard1, 2, 5  |
| 3     | shard5, 1  | shard2, 3, 4  |

Setiap ronde: 2 shard mati → 100 data diinsert ke shard hidup → shard dihidupkan kembali → sinkronisasi otomatis (event-driven)

---

## HASIL AKHIR YANG DIHARAPKAN
- Master total : **2300** (2000 + 3×100)
- Tiap shard   : **460** (2300 / 5)
- Sum shard    : **2300** = master ✓

## LAPISAN 1
docker-compose.yml (Otomatisasi Infrastruktur)
Ketika ketik docker-compose up --build, Docker membaca file ini dan sekaligus membangun dan menjalankan 6 container — 1 master + 5 shard — dalam satu jaringan virtual bernama kampus-net.
Yang paling penting di file ini:

depends_on:
  - kampus-shard1
  - kampus-shard2
  ...
Baris ini memastikan semua shard dinyalakan duluan sebelum master. Docker tidak akan jalankan master sebelum ke-5 shard siap. Ini yang bikin urutannya otomatis benar.

environment:
  - SHARD1_URL=http://kampus-shard1:5001
Baris ini memberitahu master di mana alamat tiap shard di dalam jaringan Docker. Nama kampus-shard1 langsung bisa diakses seperti nama domain karena mereka satu jaringan (kampus-net).

## LAPISAN 2
master.py (Otak Sistem / Otomatisasi Logika)
Begitu container master nyala, Python langsung jalankan fungsi main() dari atas ke bawah — tidak perlu input manual apapun:

def main():
    wait_for_shards()        # tunggu semua shard siap
    distribute_initial_data() # fase 1: sharding
    replicate_all()           # fase 2: replikasi
    run_error_scenario()      # fase 3: skenario error 3 ronde
Setiap fungsi dipanggil otomatis satu per satu. Inilah kenapa begitu Docker jalan, semuanya langsung berjalan sendiri tanpa kamu perlu ketik apapun lagi.

## LAPISAN 3
shard.py (Server HTTP Tiap Shard)
Setiap shard adalah server Flask yang terus menyala dan menunggu perintah dari master. Ketika master memanggil http://kampus-shard1:5001/insert, shard langsung merespons. Komunikasi inilah yang bikin master bisa mengontrol semua shard dari satu tempat.


