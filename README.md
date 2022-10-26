# 2207-Distributed-File-System

This project was for COMP2207 Distributed Systems and Networks Cousework 2021/22.

The aim of this project was to create a Distributed Storage System.

## How it works
### Controller
```
java Controller cport R timeout rebalance_period
```
The _timeout_ is in milliseconds and the _rebalance\_period_ is in seconds.

The Controller keeps track of where files are, and manages the balancing of files between Dstores.
The client will communicate with the Controller for all operations and the Controller will direct the client to a Dstore.

No file data passes through the controller. All data is sent directly from the client to the Dstores.

### Dstores
```
java Dstore port cport timeout file_folder
```
The _timeout_ is in milliseconds. Data will be stored in the _file\_folder_.

Dstores wait for requests from the Controller or the Client and respond accordinly.


### Client
```
java Client cport timeout
```

### Operations
* Store
* Load
* List
* Remove
* Rebalance (Started periodically by controller and when a new Dstore joins)
