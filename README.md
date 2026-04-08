# py_nats_uos

This project targets Weidmuller u-OS controllers and demonstrates Python-based integration with the u-OS Data Hub over NATS.

The root Python entry point is autostart.py. It contains an example implementation for:

- a Raffstore switch (up/down control with safety timing)
- an ordinary/standard switch (toggle behavior with optional auto-off timer)

## What the project does

- Authenticates against u-OS using client credentials
- Connects to NATS and reads provider variable definitions
- Subscribes to variable-change events from the Data Hub
- Maps digital input changes to digital output commands
- Supports two control patterns in building.py:
  - Switch: toggles output on input edge, optional auto-off timer
  - Raffstore: coordinated up/down logic with delay and runtime windows

## Main files

- autostart.py: startup and wiring of example variables and controller behavior
- building.py: high-level control logic (Switch, Raffstore, timer behavior)
- src/data_hub.py: Data Hub access layer (definition read, value read/write, subscriptions)
- src/nats_connection.py: NATS connection helper
- src/nats_authent.py: OAuth/client credentials token handling

## Setup

1. Create and activate a Python virtual environment.
2. Install dependencies:

   pip install -r requirements.txt

3. Create a local login file from the sample and fill your real credentials.
4. Run:

   python autostart.py

## Testing

The project includes unit tests to verify the behavior of the building control classes.

### Running Tests

1. Ensure you have activated the virtual environment and installed dependencies as described in Setup.
2. Run the tests using pytest:

   python -m pytest tests/ -v

### Test Coverage

- `tests/test_building.py`: Tests for the Building base class and Raffstore/Switch subclasses
  - Verifies that Raffstore instances are independent (not singletons)
  - Confirms shared access provider behavior across instances
  - Tests timer and state management isolation

## Credentials

A sample credentials file is included as login.sample.py.

For runtime, create login.py with your real values (this file is ignored by Git to avoid leaking secrets).

## Create Client Credentials On The Controller

To access the NATS/Data Hub API with this project, create a client in the u-OS Control Center:

1. Open u-OS Control Center on the controller.
2. Go to Identity & access -> Clients.
3. Click Add client.
4. Create a client (for example `sampleprovider`) and store:
   - Client ID
   - Client secret
5. Configure scopes required by this project, typically:
   - `hub.variables.provide`
   - `hub.variables.readwrite`
6. Copy the values into login.py:
   - CLIENT_NAME = client name
   - CLIENT_ID = generated client ID
   - CLIENT_SECRET = generated client secret

Reference screenshots are available in doc/:

- doc/IoTUeli-Datahub.gif
- doc/IoTUeli-u-OS.gif

Source attribution for these two screenshots: [uiff/nats-python-uc20](https://github.com/uiff/nats-python-uc20)

## Running From A Local PC

- This sample project was also successfully run from a local PC using SSH tunneling to the controller.
- Replace CONTROLLER_ADDRESS with the IP address or hostname of your controller.
- Tunnel the controller web interface (HTTPS):

   ssh -L 443:127.0.0.1:443 admin@CONTROLLER_ADDRESS

- Tunnel the NATS service (default project port 49360):

   ssh -L 49360:127.0.0.1:49360 admin@CONTROLLER_ADDRESS

- In the controller web UI, unsecure access had to be enabled for this setup.

## Notes

- This example is intended to run directly on the controller. For this, the Python module nats must be installed on the controller.
- Variable key examples are in autostart.py.
- You can switch from Raffstore to standard switch behavior by changing the setup calls in autostart.py.
