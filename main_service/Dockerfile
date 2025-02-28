FROM python:3.11

ADD . .
RUN python3.11 -m pip install -e .

# No entrypoint defined because then we would need to define a port.
# And the port changes constantly, so we just run the command directly / programmatically.