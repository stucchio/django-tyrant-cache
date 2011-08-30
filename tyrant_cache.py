try:
    import cPickle as pickle
except ImportError:
    import pickle

import pytyrant
import time
import socket

from django.core.cache.backends.base import BaseCache
from django.utils.encoding import smart_unicode, smart_str

def __retry_with_reset_on_error__(func):
    def wrapped(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except socket.error, e: #Both these errors can be raised on various errors
            self.reset_cache_connection()
            return func(self, *args, **kwargs)
        except pytyrant.TyrantError, e:
            self.reset_cache_connection()
            return func(self, *args, **kwargs)
    return wrapped


class CacheClass(BaseCache):
    def __init__(self, server, params, timeout=0.5):
        "Connect to Tokyo Tyrant, and set up cache backend."
        BaseCache.__init__(self, params)
        host, port = server.split(':')
        self.host = host
        self.port = int(port)
        self.timeout = timeout
        self.reset_cache_connection()

    def reset_cache_connection(self):
        sock = socket.socket()
        sock.connect((self.host, self.port))
        sock.settimeout(self.timeout)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._cache = pytyrant.Tyrant(sock)

    def _prepare_key(self, raw_key):
        return smart_str(raw_key)

    @__retry_with_reset_on_error__
    def add(self, key, value):
        "Add a value to the cache. Returns ``True`` if the object was added, ``False`` if not."
        try:
            value = pickle.dumps(value)
            self._cache.putkeep(self._prepare_key(key), value)
        except pytyrant.TyrantError:
            return False
        return True

    @__retry_with_reset_on_error__
    def get(self, key, default=None):
        "Retrieve a value from the cache. Returns unpicked value if key is found, 'default' if not. "
        try:
            value = self._cache.get(self._prepare_key(key))
        except pytyrant.TyrantError:
            return default

        value = pickle.loads(value)
        if isinstance(value, basestring):
            return smart_unicode(value)
        else:
            return value

    @__retry_with_reset_on_error__
    def set(self, key, value, timeout=0):
        "Persist a value to the cache."
        value = pickle.dumps(value)
        self._cache.put(self._prepare_key(key), value)
        return True

    @__retry_with_reset_on_error__
    def delete(self, key):
        "Remove a key from the cache."
        try:
            self._cache.out(self._prepare_key(key))
        except pytyrant.TyrantError: #Should not raise error if key doesn't exist
            return None

    @__retry_with_reset_on_error__
    def get_many(self, keys):
        "Retrieve many keys."
        many = self._cache.mget(keys)
        return dict([( k, pickle.loads(v)) for k,v in many])

    @__retry_with_reset_on_error__
    def set_many(self, d):
        "Set many keys."
        #Because I can't figure out how to implement an efficient set_many, I implement an inefficient one
        #The main goal here is not to break code which demands this.
        for k in d.keys():
            self.set(k, d[k])
        return True

    @__retry_with_reset_on_error__
    def flush(self, all_dbs=False):
        self._cache.vanish()

    @__retry_with_reset_on_error__
    def incr(self, key, delta=1):
        "Atomically increment ``key`` by ``delta``."
        return self._cache.addint(self._prepare_key(key), delta)
