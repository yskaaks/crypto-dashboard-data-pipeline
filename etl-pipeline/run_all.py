import subprocess
import threading
import time
import os

def run_etl():
    subprocess.run(["poetry", "run", "python", "etl_pipeline/main.py"])

def run_web_app():
    os.environ['FLASK_APP'] = 'web_app.app'
    os.environ['FLASK_ENV'] = 'development'
    subprocess.run(["poetry", "run", "flask", "run", "--host=0.0.0.0"])

if __name__ == "__main__":
    etl_thread = threading.Thread(target=run_etl)
    web_thread = threading.Thread(target=run_web_app)

    etl_thread.start()
    time.sleep(5)  # Give ETL some time to start
    web_thread.start()

    etl_thread.join()
    web_thread.join()