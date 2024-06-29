from flask import Flask, render_template, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Configure database connection
db_config = {
    'host': os.getenv('RDS_HOST'),
    'database': os.getenv('RDS_DB_NAME'),
    'user': os.getenv('RDS_USERNAME'),
    'password': os.getenv('RDS_PASSWORD')
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/crypto_data')
def get_crypto_data():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM crypto_data ORDER BY market_cap DESC LIMIT 10")
    data = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)