import psycopg2

conn = psycopg2.connect(
    dbname="corrupcion_db",
    user="admin",
    password="td8corrupcion",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute("SELECT 1;")
print(cur.fetchone())
cur.close()
conn.close()
