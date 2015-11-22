from functools import update_wrapper, partial
from inspect import getargspec as _getargspec

def getargspec(func):
    return _getargspec(unwrap(func))

def _wrap(wrapper, wrapped):
    update_wrapper(wrapper=wrapper, wrapped=wrapped)
    wrapper._wrapped = unwrap(wrapped)
    return wrapper

def wraps(func):
    wrapper = partial(_wrap, wrapped=func)
    return wrapper

def unwrap(func):
    wrapped_func = func
    while hasattr(wrapped_func, '_wrapped'):
        wrapped_func = wrapped_func._wrapped

    return wrapped_func

def populate_args(method, sent_args, custom_args):
    kwargs = {}
    arg_spec = getargspec(method).args
    for i, arg_name in enumerate(arg_spec):
        if i == 0 and (arg_name == 'self' or arg_name == 'cls'):
            continue # these don't get passed in
        if arg_name in custom_args:
            kwargs[arg_name] = custom_args[arg_name]
        elif arg_name in sent_args and not arg_name.startswith('_'):
            kwargs[arg_name] = sent_args[arg_name]
    
    return kwargs
