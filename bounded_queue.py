import hazelcast
import threading

def write_to_queue():
    client = hazelcast.HazelcastClient(cluster_name="haz_hw1")
    queue = client.get_queue("bounded-queue").blocking()
    print("Writing to queue...")
    for i in range(1, 101):
        queue.put(i)
        print("Wrote:", i)
    print("Finished writing to queue.")
    client.shutdown()

def read_from_queue():
    client = hazelcast.HazelcastClient(cluster_name="haz_hw1")
    queue = client.get_queue("bounded-queue")
    while True:
        future = queue.poll()
        if future is not None:
            value = future.result()  # Wait for the result
            print("Read:", value)
        else:
            print("Queue is empty")
            break
    client.shutdown()

import time

def read_from_queue(num):
    client = hazelcast.HazelcastClient(cluster_name="haz_hw1")
    queue = client.get_queue("bounded-queue").blocking()
    num_empty = 0
    while True:
        future = queue.poll()
        if future is not None:
            # value = future.result()
            print(f"Read from {num}: {future}")
            num_empty = 0
        else:
            # time.sleep(0.01)
            num_empty += 1
        if num_empty > 10:
            print("Queue is empty for ", num)
            break
    client.shutdown()


if __name__ == "__main__":
    # Start writer client
    write_thread = threading.Thread(target=write_to_queue)
    write_thread.start()

    # Start reader clients
    read_thread1 = threading.Thread(target=read_from_queue, args=(1,))
    read_thread2 = threading.Thread(target=read_from_queue, args=(2,))
    read_thread1.start()
    read_thread2.start()

    # Wait for threads to finish
    write_thread.join()
    read_thread1.join()
    read_thread2.join()
