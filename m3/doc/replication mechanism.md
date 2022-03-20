## something to do for implementing chain replication mechanism

### In KVServer

- **propagate to the next replica in function putKV**

  - KVServer should know who is successor（metadata？）

  - KVServers should have ability to directly communicate with each other.

    the code in “DataMigrationManager” might help

    - send replicate request 
    - reply with ack

- **two more database for a KVServer**

  each KVServer is a coordinator and two replica, need two more replica database, play as Stable History role.

  only the tail replica should use cache for getKV

  <img src="replication mechanism.assets/image-20220320015450631.png" alt="image-20220320015450631" style="zoom: 67%;" />

- **queue structure and serial numbers for each put operation**

  use a queue to keep tracking all the un-ack values，like the sliding window protocol. 

  the queue is the subset of Speculative History and Stable History

  is serial number necessary?

### In KVStore

- should connect head replica for put operation，and wait for response from tail

- connect tail for get replica operation.

### In ECS

- **heart beat of KVServer**

  for detecting failure of a KVServer( is it necessary? )

  when a KVServer fail, we can treat it like a node is removed

- **addnode**

  rechain the replica chain

  - insert  i
    1. make i a tail replica for i-2 by coping the (i-1 for i-2) database
    2. make i a middle replica for i-1 by coping the 
    3. delete two tail replica: (i+2 for i-1), (i+1 for i-2)
    4. 
    5. 

  

- **removenode**

  