#### Container Service

This is a component of the open-source cluster compute software [Burla](https://github.com/Burla-Cloud/burla).

The "container service" is a flask webservice that runs inside each Docker container in the cluster, including custom containers submitted by users.
This service is the lowest abstraction layer in the Burla stack, it's the service that actually executes user-submitted functions, while streaming stdout/errors back the database where they are picked by by the [main_service](https://github.com/Burla-Cloud/main_service) and sent to the client.
