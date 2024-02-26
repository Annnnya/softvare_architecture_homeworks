import hazelcast
import time
import threading
import sys

def run_client(lock_type):
    client = hazelcast.HazelcastClient(
        cluster_name="haz_hw1"
    )
    
    distributed_map = client.get_map("my-distributed-map").blocking()
    
    if lock_type == "none":
        for _ in range(10000):
            val = distributed_map.get("key")
            distributed_map.put("key", val+1)

    elif lock_type == "pessimistic":
        for _ in range(10000):
            distributed_map.lock("key")
            try:
                val = distributed_map.get("key")
                distributed_map.put("key", val + 1)
            finally:
                distributed_map.unlock("key")

    elif lock_type == "optimistic":
        for _ in range(10000):
            while True:
                val = distributed_map.get("key")
                updated_val = val + 1
                if distributed_map.replace_if_same("key", val, updated_val):
                    break

    client.shutdown()

if __name__ == "__main__":
    lock_type = sys.argv[1] if len(sys.argv) > 1 else "none"
    
    client = hazelcast.HazelcastClient(
        cluster_name="haz_hw1"
    )
    distributed_map = client.get_map("my-distributed-map")
    distributed_map.lock("key")
    distributed_map.put("key", 0)
    distributed_map.unlock("key")
    time.sleep(1)
    
    lock_types = ["none", "pessimistic", "optimistic"]
    if lock_type not in lock_types:
        print("Invalid lock type. Please choose one of:", ", ".join(lock_types))
        sys.exit(1)

    threads = []
    for _ in range(3):
        t = threading.Thread(target=run_client, args=(lock_type,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("All done. Result:", distributed_map.get("key").result())
    client.shutdown()
