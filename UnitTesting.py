import struct


#gw nyoba nyoba buat tedting hehe


# === HEADER INFO ===
num_records = 3
record_format = 'i20sf'  # int (4), string (20), float (4)
record_size = struct.calcsize(record_format)
num_columns = 3

# === METADATA ===
columns = [
    ("id", "INT", 4),
    ("name", "STR", 20),
    ("gpa", "FLOAT", 4)
]

# === DATA ===
records = [
    (1, "Alice", 3.75),
    (2, "Bob", 3.40),
    (3, "Charlie", 3.90)
]

# === WRITE TO FILE ===
with open("students.dat", "wb") as f:
    # Write HEADER: 3 integers
    f.write(struct.pack("iii", num_records, record_size, num_columns))

    # Write METADATA: each column name/type padded to 20 bytes + int length
    for name, dtype, length in columns:
        name_bytes = name.encode('utf-8').ljust(20, b'\x00')
        dtype_bytes = dtype.encode('utf-8').ljust(20, b'\x00')
        f.write(struct.pack("20s20si", name_bytes, dtype_bytes, length))

    # Write DATA
    for sid, name, gpa in records:
        encoded_name = name.encode('utf-8')[:20].ljust(20, b'\x00')
        f.write(struct.pack(record_format, sid, encoded_name, gpa))

print("✅ students.dat successfully created with header + metadata + data")

with open("students.dat", "rb") as f:
    # === READ HEADER ===
    header_format = "iii"
    num_records, record_size, num_columns = struct.unpack(header_format, f.read(struct.calcsize(header_format)))
    print(f"Header -> Records: {num_records}, Record size: {record_size}, Columns: {num_columns}")

    # === READ METADATA ===
    columns = []
    for _ in range(num_columns):
        name_bytes, dtype_bytes, length = struct.unpack("20s20si", f.read(44))
        name = name_bytes.decode('utf-8').rstrip('\x00')
        dtype = dtype_bytes.decode('utf-8').rstrip('\x00')
        columns.append((name, dtype, length))
    print("Metadata:", columns)

    # === READ DATA ===
    record_format = 'i20sf'
    for i in range(num_records):
        sid, raw_name, gpa = struct.unpack(record_format, f.read(struct.calcsize(record_format)))
        name = raw_name.decode('utf-8').rstrip('\x00')
        print(f"Row {i+1}: ID={sid}, Name={name}, GPA={gpa}")
