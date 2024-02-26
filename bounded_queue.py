import hazelcast
import threading

def write_to_queue():
    client = hazelcast.HazelcastClient(cluster_name="haz_hw1")
    queue = client.get_queue("bounded-queue")
    for i in range(1, 101):
        queue.put(i)
    client.shutdown()

def read_from_queue():
    client = hazelcast.HazelcastClient(cluster_name="haz_hw1")
    queue = client.get_queue("bounded-queue")
    while True:
        value = queue.poll()
        if value is not None:
            print("Read:", value)
        else:
            print("Queue is empty")
            break
    client.shutdown()

if __name__ == "__main__":
    # Create bounded queue with size 10
    client = hazelcast.HazelcastClient(cluster_name="haz_hw1")
    config = client.get_queue("bounded-queue").get_config()
    config.set_max_size(10)
    client.shutdown()

    # Start writer client
    write_thread = threading.Thread(target=write_to_queue)
    write_thread.start()

    # Start reader clients
    read_thread1 = threading.Thread(target=read_from_queue)
    read_thread2 = threading.Thread(target=read_from_queue)
    read_thread1.start()
    read_thread2.start()

    # Wait for threads to finish
    write_thread.join()
    read_thread1.join()
    read_thread2.join()
