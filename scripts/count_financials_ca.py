import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def main():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT', 5432), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM financials WHERE exchange = 'CA'")
    c = cur.fetchone()[0]
    print(f"financials rows with exchange='CA': {c}")
    cur.close(); conn.close()

if __name__ == '__main__':
    main()
