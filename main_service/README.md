#### Main Service

The "main service" is a fastapi webservice designed to be deployed in [google-cloud-run](cloud.google.com/run).  
This service acts as a traditional "head node" would, as well as handing other responsibilities.  
This service is responsible for:

- Adding/removing/managing nodes in the cluster.
- Routing requests from clients to the correct `node_service`'s, (`/burla/node_service`)
- Hosting the cluster-management dashboard

Every "main service" instance has it's own [google-cloud-firestore](cloud.google.com/firestore) database associated with it.  
It is currently not possible to run more than one "main-service" instance in any single google-cloud-project.  
It is currently not possible to run more than one "cluster" using a single "main-service".  