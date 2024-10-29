#### Node Service

This is a component of the open-source cluster compute software [Burla](https://github.com/Burla-Cloud/burla).

The "node service" is a fastapi webservice that runs inside each Node (virtual machine) in the cluster.  
This service is responsible for starting/stopping/managing Docker containers running on the node.  
It also serves as a router responsible for delegating requests to the correct container.
