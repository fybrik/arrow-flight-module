from pyarrow import flight
from pyarrow.flight import ServerAuthHandler

# taken from https://github.com/apache/arrow/blob/master/python/pyarrow/tests/test_flight.py#L508
class NoopAuthHandler(ServerAuthHandler):
    """A no-op auth handler."""

    def authenticate(self, outgoing, incoming):
        outgoing.write("")
        """Do nothing."""

    def is_valid(self, token):
        """
        Returning an empty string.
        Returning None causes Type error.
        """
        return ""

# taken from https://github.com/apache/arrow/blob/master/python/pyarrow/tests/test_flight.py#L426
class HttpBasicServerAuthHandler(ServerAuthHandler):
    """An example implementation of HTTP basic authentication."""

    def __init__(self, creds):
        super().__init__()
        self.creds = creds

    def authenticate(self, outgoing, incoming):
        buf = incoming.read()
        auth = flight.BasicAuth.deserialize(buf)
        if auth.username.decode() not in self.creds:
            raise flight.FlightUnauthenticatedError("unknown user")
        if self.creds[auth.username.decode()] != auth.password.decode():
            raise flight.FlightUnauthenticatedError("wrong password")
        outgoing.write(auth.username)

    def is_valid(self, token):
        if not token:
            raise flight.FlightUnauthenticatedError("token not provided")
        if token.decode() not in self.creds:
            raise flight.FlightUnauthenticatedError("unknown user")
        return token

class AFMAuthHandler(ServerAuthHandler):
    def __init__(self, auth_config):
        super().__init__()
        if not auth_config:
            self.auth_handler = NoopAuthHandler()
        elif 'basic' in auth_config:
            self.auth_handler = HttpBasicServerAuthHandler(auth_config['basic'].get('credentials', None))
        else:
            raise NotImplementedError("Unknown authenticaion type")

    def authenticate(self, outgoing, incoming):
        self.auth_handler.authenticate(outgoing, incoming)

    def is_valid(self, token):
        return self.auth_handler.is_valid(token)
