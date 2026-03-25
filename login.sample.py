# Sample credentials file for py_nats_uos
# Copy values into login.py for local runtime.

NATS_HOST = "127.0.0.1"
NATS_PORT = 49360

# Provider configured on the controller
PROVIDER_SBM = "u_os_sbm"

# OAuth client data from u-OS Control Center
CLIENT_NAME = "sampleprovider"
CLIENT_ID = "00000000-0000-0000-0000-000000000000"
CLIENT_SECRET = "replace-with-your-client-secret"

# Typical scope for this project
CLIENT_SCOPE = "hub.variables.provide hub.variables.readwrite"
