class RedisStore:

    def from_connection(self, connection):
        self._store = connection