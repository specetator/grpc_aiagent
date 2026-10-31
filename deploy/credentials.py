"""The two operator credentials shared with an optional public Connector."""

def connector_credentials(security, environment):
    result = {}
    for variable, name in (('SPARK_PUSH_HERMES_API_KEY', 'gateway'),
                           ('SPARK_PUSH_WORKSPACE_AUTH_KEY', 'workspace')):
        value = environment.get(variable) or security.get(name, '')
        result[variable] = value if isinstance(value, str) else ''
    return result


def valid_connector_key(value):
    return (isinstance(value, str) and 32 <= len(value) <= 256 and
            all(33 <= ord(c) <= 126 for c in value))
