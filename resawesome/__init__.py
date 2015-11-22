from decorators import create, read, update, delete, lookup, execute
from resource import resource, API

class ResourceNotImplementedError(NotImplementedError):
    pass

class Resource(object):
    @staticmethod
    @create
    def _create(_user_id, **kwargs):
        raise ResourceNotImplementedError

    @staticmethod
    def _has_class_access(_user_id, permission):
        raise ResourceNotImplementedError

    def _has_access(_user_id, permission):
        raise ResourceNotImplementedError

    def _commit():
        raise ResourceNotImplementedError

    def _serialize(_user_id, permission):
        return {}
