VERSION_PREFIX = "v1"
LOCATION_PREFIX = "loc"


def vars_changed_event(provider_id: str) -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.{provider_id}.vars.evt.changed"


def read_variables_query(provider_id: str) -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.{provider_id}.vars.qry.read"


def write_variables_command(provider_id: str) -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.{provider_id}.vars.cmd.write"


def provider_changed_event(provider_id: str) -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.{provider_id}.def.evt.changed"


def registry_provider_event(provider_id: str) -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.registry.providers.{provider_id}.def.evt.changed"


def registry_provider_query(provider_id: str) -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.registry.providers.{provider_id}.def.qry.read"


def registry_providers_query() -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.registry.providers.qry.read"


def registry_providers_changed_event() -> str:
    return f"{VERSION_PREFIX}.{LOCATION_PREFIX}.registry.providers.evt.changed"
