import threading
import ndjson
import random
import time
import names
def download_ndjson_data(thread_id, num_records=10):
    print(f"Thread {thread_id} is starting download...")
    data = []
    for _ in range(num_records):
        record = {
            "id": random.randint(1, 1000),
            "name": names.get_full_name(),
            "age": random.uniform(10.5, 99.9)
        }
        data.append(record)
        time.sleep(0.1) 
    with open(f"{thread_id}.ndjson", 'w') as f:
        ndjson_writer = ndjson.writer(f)
        for record in data:
            ndjson_writer.writerow(record)
    print(f"Thread {thread_id} finished downloading {num_records} records.\n")
threads = []
num_threads = 5  
records_per_thread = 10 
for i in range(num_threads):
    thread = threading.Thread(target=download_ndjson_data, args=(i, records_per_thread))
    threads.append(thread)
    thread.start()
for thread in threads:
    thread.join()

print("All downloads completed.")
