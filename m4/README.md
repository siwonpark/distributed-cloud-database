# Milestone 4: ACID Transactions

## Description

In Milestone 4, we extended our work from the previous milestones and created the ability to process transactions in our storage service. Namely, we enable the user to specify from the command line a set of read/write operations that they wish to “wrap” in a transaction, and we provide guarantees that that set of transactions will be carried out in an ACID manner (atomicity, consistency, isolation, durability).

This README document serves as a guide to run our code locally and try out our storage service; all instructions are
are based on the EECG machines. 

## Getting Started

### Installing

* Open our software artifacts and navigate to the `m4` directory. From now on, all commands listed below should be executed from this directory.

### Executing program

*Make sure that the local machine is clean before running our storage service!
Such as other server threads competing for the same ports, or existing data in the zookeeper instance*

*Make sure public-key-authentication is enabled, 
and that you are able to do `ssh 127.0.0.1` from the machine 
you're running on, without having to respond to any prompts*

```
# Run from /m4 directory

# The following commands build the .jars that we need to execute our programs
> ant
> ant build-ecs-jar
```

Now, we start up a KVClient: 
```
# Start up a client (we based our implementation off of the M2 code,
# hence the "m2" in the jar names)
> java -jar m2-client.jar
```

In a SEPARATE terminal, we start up the ECS (and spawn up 4 nodes): 
```
# In a SEPARATE terminal: start up the ECS
> java -jar m2-ecs.jar src/ecs/ecs.config
> addNodes 4 FIFO 20
> start

# Make sure execute shutDown/quit in the ECS when you're done 
# or else there will be zombie server threads
```

Now, back to the KVClient (we should be within the KVClient program now): 
```
# Existing features like put/get work, but the instructions below
# are specifically for executing the M4 specific code, i.e. ACID Transactions

KVClient > connect localhost <Server that is up, you can see this from ECS>
KVClient > initTransaction
# Run transactionStatus at any time to see the current transaction status
KVClient [In Transaction]> put a a 
KVClient [In Transaction]> put b b
KVClient [In Transaction]> get a
KVClient [In Transaction]> put a null
KVClient [In Transaction]> get a
KVClient [In Transaction]> put a d
KVClient [In Transaction]> get a
KVClient [In Transaction]> get b
KVClient [In Transaction]> commit

# Make sure you've executed `start` in the ECS before you `commit`
# or else the commit won't work (since the servers arent started)
```
After executing the above, you should see something similar to the below:
```
KVClient> connect localhost 50052
Connected to localhost port 50052!
KVClient> initTransaction
Transaction initiated.
KVClient [In Transaction]> put a a
Operation added to transaction.
KVClient [In Transaction]> put b b
Operation added to transaction.
KVClient [In Transaction]> get a
Operation added to transaction.
KVClient [In Transaction]> put a null
Operation added to transaction.
KVClient [In Transaction]> get a
Operation added to transaction.
KVClient [In Transaction]> put a d
Operation added to transaction.
KVClient [In Transaction]> get a
Operation added to transaction.
KVClient [In Transaction]> get b
Operation added to transaction.
KVClient [In Transaction]> commit
Success! Inserted {"a": "a"} to the database.
Success! Inserted {"b": "b"} to the database.
Success! Retrieved value "a" from the database.
Success! Deleted key "a" from the database.
There is no entry with key: "a" in the database.
Success! Inserted {"a": "d"} to the database.
Success! Retrieved value "d" from the database.
Success! Retrieved value "b" from the database.
Success! Commit Successful!
```

You can then check the state after the atomic transaction, 
that all keys and values are as expected after the transaction completes. Here's an example where we check the keys
`a` and `b` since those are the ones in the example above: 
```
KVClient> get a
Success! Retrieved value "d" from the database.
KVClient> get b
Success! Retrieved value "b" from the database.
```

As we can see, the values are as expected when considering the transaction as an atomic operation.

## Testing

Running `ant test` from the `/m4` directory will run our full suite of tests, 
which includes the new tests we added in M4, and the tests from previous milestones.


## Help

`help` contains information about the command line interface
for both ECS and KVClient


## Acknowledgments

This README template was inspired by the template authored at
https://gist.github.com/DomPizzie/7a5ff55ffa9081f2de27c315f5018afc