
import sqlite3
import os

db_path = "/Volumes/PRO-G40/gitclones/Gantry/benchmark.db"
if not os.path.exists(db_path):
    print(f"DB {db_path} does not exist!")
    exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

def count(table):
    row = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    print(f"{table}: {row[0]}")

print("--- DB Counts ---")
count("patients")
count("studies")
count("series")
count("instances")

print("\n--- Patients ---")
for r in cur.execute("SELECT * FROM patients"):
    print(dict(r))

print("\n--- Linked Instances Query Check (First 1) ---")
query = """
    SELECT 
        p.patient_id, i.sop_instance_uid
    FROM instances i
    JOIN series s ON i.series_id_fk = s.id
    JOIN studies st ON s.study_id_fk = st.id
    JOIN patients p ON st.patient_id_fk = p.id
"""
res = cur.execute(query).fetchall()
print(f"Query returned {len(res)} rows.")
if res:
    print(dict(res[0]))

conn.close()
