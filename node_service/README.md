#### Node Service

The "node service" is a fastapi webservice that runs inside each Node (virtual machine) in the cluster.  
This service is responsible for starting/stopping/managing workers running on the node.  
It also serves as a router responsible for directing requests to the correct workers.
