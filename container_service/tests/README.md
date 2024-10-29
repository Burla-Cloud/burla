### How to test the container service

This service has no test suite, to test it we run the tests for the node service.
If these tests fail due to an error in the container service, the node service should print the logs from the container that failed. If this doesn't happen for some reason the logs will also be available through `docker logs ...` or google cloud logging.

To modify the container service we run `make image_nogpu` or `make image_gpu` which will re-bake the code into a new image (takes about 40s to run). The node service will pull the latest image next time the tests are run.
These images build quickly because they only add application code to a separate base image (`make base_image_nogpu`) that already exists and does not need to be updated.

1. In the node service run `make test`
2. Observe the container service logs in stderr or `docker logs ...`.
3. Fix errors,
    (you do NOT need to make a new container every iteration,
    code is mounted into existing container when `IN_DEV`)
3. Once passing, bake code into a new container buy running `make image_nogpu` or `make image_gpu`
