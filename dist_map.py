import hazelcast
client = hazelcast.HazelcastClient(
  cluster_name="haz_hw1", 
  ) 
distributed_map = client.get_map("my-distributed-map")

for i in range(1000):
    distributed_map.put("key-" + str(i), "value-" + str(i))
