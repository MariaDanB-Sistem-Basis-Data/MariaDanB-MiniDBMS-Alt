# MariaDanB-MiniDBMS
Ini adalah mini DBMS yang dikembangkan sebagai proyek akhir mata kuliah IF3140 Sistem Basis Data di Institut Teknologi Bandung. Tersusun atas lima komponen utama basis data: query processing, storage management, concurrency control, failure recovery, dan query optimization. Sistem ini dirancang dengan arsitektur klien-server yang memungkinkan banyak klien untuk terhubung ke server basis data secara bersamaan.

## Prasyarat
- **Python 3.8+**

## Cara Instal
1. Clone repo ini
   ```bash
   git clone https://github.com/MariaDanB-Sistem-Basis-Data/MariaDanB-MiniDBMS-Alt.git
   cd MariaDanB-MiniDBMS-Alt
   ```

2. Inisialisasi basis data dengan data sampel:
   ```bash
   python storage_manager/storagemanager_helper/init.py
   ```

3. Jalankan server
   ```bash
   python server.py
   ```
   Server berjalan di `127.0.0.1:13523`

4. Sambungkan klien
   ```bash
   python client.py --interactive
   ```

## Beberapa Cara Menggunakan Klien

```bash
# Interactive mode (default)
python client.py --interactive

# Connect to custom host/port
python client.py --host 192.168.1.100 --port 8080 --interactive

# Run single query
python client.py --batch "SELECT * FROM Student;"

# Show help
python client.py --help
```

### Kueri SQL

```sql
-- Create table
CREATE TABLE Student (StudentID INTEGER, FullName VARCHAR(100), GPA FLOAT);

-- Insert data
INSERT INTO Student (StudentID, FullName, GPA) VALUES (1, 'Alice Johnson', 3.9);

-- Query data
SELECT * FROM Student;
SELECT FullName, GPA FROM Student WHERE GPA > 3.5;

-- Update data
UPDATE Student SET GPA = 4.0 WHERE StudentID = 1;

-- Transaction
BEGIN TRANSACTION;
UPDATE Student SET GPA = 3.95 WHERE StudentID = 1;
COMMIT;

-- Join
SELECT S.FullName, C.CourseName 
FROM Student S 
JOIN Attends A ON S.StudentID = A.StudentID 
JOIN Course C ON A.CourseID = C.CourseID;
```

### Command Khusus
Di mode interaktif klien, tersedia beberapa perintah khusus:

| Command                  | Description                        |
| ------------------------ | ---------------------------------- |
| `\dt`                    | Tampilkan semua tabel              |
| `\d <table>`             | Struktur tabel dan statistik       |
| `\tx` or `\transactions` | Lihat transaksi aktif              |
| `explain <query>`        | Lihat query plan dan analisis cost |
| `\checkpoint`            | Trigger checkpoint di server       |
| `\ping`                  | cek koneksi server                         |
| `\help`                  | Show help                  |
| `exit` or `quit`         | Disconnect & exit                |

**Contoh:**
```sql
SQL> \dt
  Tables:
  +--------------------------------+
  | Table Name                     |
  +--------------------------------+
  | Attends                        |
  | Course                         |
  | Student                        |
  +--------------------------------+

SQL> \d Student
  Table: Student
  +----------------------+-----------------+------------+
  | Column               | Type            | Key        |
  +----------------------+-----------------+------------+
  | StudentID            | INTEGER         |            |
  | FullName             | VARCHAR(100)    |            |
  | GPA                  | FLOAT           |            |
  +----------------------+-----------------+------------+
  
  Statistics:
    Rows: 8
    Blocks: 1
    Block factor: 8

SQL> explain SELECT * FROM Student WHERE GPA > 3.5;
  Query: SELECT * FROM Student WHERE GPA > 3.5
  
  ======================================================================
  
  Estimated Cost (before optimization): 245.5
  Estimated Cost (after optimization): 123.2
  Cost reduction: 49.8%
  
  Optimization Details:
      Method: Heuristic + Cost-based
      Heuristics: Push selection, Early projection
  
  ======================================================================

SQL> \tx
  +----------------------+-----------------+---------------------------+
  | Transaction ID       | Status          | Start Time                |
  +----------------------+-----------------+---------------------------+
  | 1                    | ACTIVE          | 2025-12-06 22:30:15       |
  +----------------------+-----------------+---------------------------+
  Total: 1 active transaction(s)
```


## Struktur

```
MariaDanB-MiniDBMS/
├── server.py                      # Server database
├── client.py                      # Client database
├── init_data.py                   # Database initialization
├── MiniDBMS.py                    # Core DBMS coordinator
├── bootstrap.py                   # Dependency injection
├── cli.py                         # Standalone CLI
├── main.py                        # Entry point
├── docker-compose.yml             # Docker orchestration
├── query_processor/               # SQL query processing
├── query_optimizer/               # Query optimization
├── storage_manager/               # Data storage & indexing
├── concurrency_control_manager/   # Transaction management
├── failure_recovery_manager/      # Recovery & logging
└── data/                          # Database files
```

