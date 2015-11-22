import functools
from util import wraps

READ    = 'read'
WRITE   = 'write'

# default access permissions
DEFAULT_ACCESS = {
    # instance methods
    'read'   : READ,
    'update' : WRITE,
    'delete' : WRITE,

    # class methods
    'create' : WRITE,
    'lookup' : READ,
    'execute': WRITE
}

def _make_decorator(maybe_func_or_access, types, method_type):
    decorated_func = None

    if callable(maybe_func_or_access):
        decorated_func = maybe_func_or_access
    elif access is None and maybe_func_or_access is not None:
        access = maybe_func_or_access
    else:
        access = DEFAULT_ACCESS[method_type]

    def _dec(func):
        @wraps(func)
        def _wrapped(*args, **kwargs):
            return func(*args, **kwargs)

        _wrapped._is_exported = True
        _wrapped._permission  = permission
        _wrapped._method_type = method_type
        _wrapped._arg_types   = types

        return _wrapped

    return _dec if decorated_func is None else _dec(decorated_func)

def create(_arg=None, **types):
    return _make_decorator(_arg, types, 'create')

def read(_arg=None, **types):
    return _make_decorator(_arg, types, 'read')

def update(_arg=None, **types):
    return _make_decorator(_arg, types, 'update')

def delete(_arg=None, **types):
    return _make_decorator(_arg, types, 'delete')

def lookup(_arg=None, **types):
    return _make_decorator(_arg, types, 'lookup')

def execute(_arg=None, **types):
    return _make_decorator(_arg, types, 'execute')
