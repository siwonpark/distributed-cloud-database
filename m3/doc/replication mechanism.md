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

  - only the tail replica should use cache for getKV
  - let putKV operate on coordinator database
  - let getKV operate on replica2 database
  - make read and write lock work on single database

  <img src="replication mechanism.assets/image-20220320015450631.png" alt="image-20220320015450631" style="zoom: 67%;" />

- **queue structure and serial numbers for each put operation**

  use a queue to keep tracking all the un-ack values，like the sliding window protocol. 

  the queue is the subset of Speculative History and Stable History

  is serial number necessary?

### In KVStore

- should connect head replica for put operation，and wait for response from tail
- connect tail for get replica operation.

### In ECSClient

- **On start**

  before start any KV Servers, there should be at least three servers on the ring and start them at the same time.

- **On stop**

  stop all the servers if after this stop there are less than three servers on the ring.

### In ECS

- **heart beat of KVServer**

  for detecting failure of a KVServer( is it necessary? )

  when a KVServer fail, we can treat it like a node is removed

- **addnode**

  rechain the replica chain

  - insert  i
    1. stop all the putKv on i+1
    2. rechain (i-2, i-1, i+1)/(coordinator, replica1, replica2)
       1. stop the putkv on i-2, wait for consistency being achieved among (i-2, i-1, i+1)
       2. make i a replica2 for i-2 by coping the i-1's replica1 database
       3. start coordinator database on i-2, start replica2 on i, handover all the getkv operations from i+1 to i
       4. delete replica2 on i+1
    3. rechain  (i-1, i+1, i+2)/(coordinator, replica1, replica2)
       1. stop the putkv on i-1, wait for consistency being achieved
       2. make i a replica1 for i-1 coordinator by coping the  i+1's  replica1 database
       3. stop the getkv on i+1, move i+1's replica1 database to replica2.
       4. start coordinator database on i-1, start replica1 on i, handover all the getkv operations from i+2 to i+1
       5. delete replica2 on i+2
    4. wait until consistency being achieved among (i+1, i+2, i+3)
    5. copy the kv pairs in range range(i-1, i] from i+1 using them init the coordinate database on i
    6. init replica1 on i+1, replica2 on i+2 by coping coordinate database on i
    7. start coordinator database on i, start replica1 on i+1, start replica2 on i+2

  

- **removenode**

  