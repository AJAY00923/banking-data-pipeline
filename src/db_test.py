import psycopg2


def test_connection():
    conn = psycopg2.connect(
        dbname="banking_pipeline",
        user="ajaygopavarapu",
        host="localhost",
        port="5432"
    )

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions;")
    row_count = cur.fetchone()[0]

    print("Connected successfully")
    print("Row count in transactions table:", row_count)

    cur.close()
    conn.close()


if __name__ == "__main__":
    test_connection()