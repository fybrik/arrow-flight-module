import pyarrow.flight as fl
from pyarrow.lib import tobytes

class AFMAuthHandler(fl.ServerAuthHandler):
    def __init__(self, auth_config):
        super().__init__()
        if not auth_config:
            self.type = 'none'
        elif 'basic' in auth_config:
            self.type = 'basic'
            self.creds = auth_config['basic'].get('credentials', None)
        else:
            raise NotImplementedError("Unknown authenticaion type")

    def authenticate(self, outgoing, incoming):
        buf = incoming.read()
        auth = fl.BasicAuth.deserialize(buf)
        if self.type == 'basic':
            if str(auth.username.decode()) not in self.creds:
                raise fl.FlightUnauthenticatedError("unknown user")
            if self.creds[auth.username.decode()] != auth.password.decode():
                raise fl.FlightUnauthenticatedError("wrong password")
        outgoing.write(tobytes(auth.username))

    def is_valid(self, token):
        if self.type == 'basic':
            if not token:
                raise fl.FlightUnauthenticatedError("token not provided")
            if token.decode() not in self.creds:
                raise fl.FlightUnauthenticatedError("unknown user")
        return token
