#### Container Service

The "container service" is a flask webservice that runs inside each Docker container in the cluster.
This service is the lowest abstraction layer in the Burla stack, it's the service that actually executes user-submitted functions, while streaming stdout/errors/results back to the client.
