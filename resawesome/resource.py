import functools
import collections

from util import populate_args
from serialization import get_encoder

DEFAULT_ROOT = None
DEFAULT_COMMIT_METHOD_NAME = '_commit'
DEFAULT_ACCESS_METHOD_NAME = '_has_access'
DEFAULT_CLASS_ACCESS_METHOD_NAME = '_has_class_access'
DEFAULT_SERIALIZATION_METHOD_NAME = '_serialize'
DEFAULT_ACCESS_LEVEL_METHOD_NAME = '_access_level'
DEFAULT_PERMISSION_ORDER = ['write', 'read']

class ResourceNotFoundError(ImportError):
    pass

class ResourceMethodNotFoundError(AttributeError):
    pass

class ResourceAccessDeniedError(Exception):
    pass

class ResourceNotAllowedError(Exception):
    pass

class API(object):
    def __init__(
        self,
        module_root=None,
        commit_method_name=DEFAULT_COMMIT_METHOD_NAME,
        access_method_name=DEFAULT_ACCESS_METHOD_NAME,
        class_access_method_name=DEFAULT_CLASS_ACCESS_METHOD_NAME,
        serialization_method_name=DEFAULT_SERIALIZATION_METHOD_NAME,
        access_level_method_name=DEFAULT_ACCESS_LEVEL_METHOD_NAME,
        permission_order=DEFAULT_PERMISSION_ORDER
    ):
        self.module_root = module_root
        self.commit_method_name = commit_method_name
        self.access_method_name = access_method_name
        self.class_access_method_name = class_access_method_name
        self.serialization_method_name = serialization_method_name
        self.permission_order = permission_order

        self.method_names = [commit_method_name, access_method_name, class_access_method_name, serialization_method_name]

        self.exports = {}
        self.locations = {}
        self.is_transactional = {}

    def resource(self, cls, export_name=None, is_transactional=True, location=None):
        # configurable decorator to apply to resource classes, to add them to this API
        if cls is None:
            return functools.partial(self.resource, export_name=export_name, is_transactional=is_transactional, location=location)

        if location is None:
            if self.module_root is not None:
                module_location = re.sub(r'^' + self.module_root, '', cls.__module__)
            elif :
                module_location = cls.__module__

        cls._IS_RESOURCE = True

        self.exports[export_name] = cls
        self.locations[module_location] = cls
        self.is_transactional[export_name] = is_transactional

        return cls

    def _access_level(self, obj, env_arguments):
        # determine if the object has a method to provide the 
        # highest level of access this environment can give
        access_level_method = getattr(obj, self.access_level_method_name, None)
        if access_level_method is not None:
            access_level_kwargs = populate_args(access_level_method, env_arguments)
            access_level = access_level_method(**access_level_kwargs)
        else:
            # fall back on calling its access method in the provided
            # permissions order
            access_method = getattr(obj, self.access_method_name, None)
            access_level = None

            if access_method is not None:
                for permission in self.permission_order:
                    access_kwargs = populate_args(access_method, {'permission': permission}, env_arguments)
                    if access_method(**access_kwargs):
                        access_method = permission
                        break

        return access_level

    def encode(self, obj, env_arguments):
        def _encode(inner_obj):
            encoded = inner_obj

            # encode resource objects with their specified serializer,
            # according to the highest level of access which the environment arguments allow
            if getattr(inner_obj, '_IS_RESOURCE', False):
                # determine the access level in this environment
                permission = self._access_level(obj, env_arguments)
                # retrieve the serializer
                serializer = getattr(inner_obj, self.serialization_method_name, None)
                
                if serializer is not None:
                    # encode the resource using its serializer with the provided permission
                    serializer_kwargs = populate_args(serializer, {'permission': permission}, env_arguments)
                    encoded = serializer(**serializer_kwargs)
                else:
                    raise ValueError("Unable to serialize: object '" + obj.__name__ + "' has no method '" + self.serialization_method_name + "'")
            elif isinstance(inner_obj, collections.Mapping):
                # recurse on dictionary-like objects
                encoded = {}
                for key, val in inner_obj.iteritems():
                    encoded[key] = _encode(val)
            elif isinstance(inner_obj, collections.Iterable):
                # recurse on array-like objects
                encoded = []
                for val in inner_obj:
                    encoded.append(_encode(val))

            return encoded

        return _encode(obj)

    def _get_resource(self, export_name):
        # look up the resource class
        resource_class = self.exports.get(export_name, None)
        if resource_class is None:
            raise ResourceNotFoundError("'" + export_name + "' is not defined as an exported resource")

        return resource_class

    def _call(self, class_name, parent, methods, access_method_name, env_arguments, allowed_method_types, encode=True):
        # look up the access method to check for access
        access_method = getattr(parent, access_method_name, None)
        if access_method is None:
            raise ResourceMethodNotFoundError("'" + class_name + "' is missing an access method")

        # check if this is an acceptable method of execution
        if method._method_type not in allowed_method_types:
            raise ResourceNotAllowedError("'" + class_name + "' is not allowed to access '" + method_name + "' in this manner")

        # check that access can be granted to call this method
        permission = method._permission
        access_kwargs = populate_args(access_method, {'permission': permission}, env_arguments)
        if not access_method(**access_kwargs):
            raise ResourceAccessDeniedError("'" + class_name + "' has denied access to '" + method_name + "'")

        result = []

        for method_data in methods:
            if isinstance(method_data, basestring):
                method_name = method_data
                sent_arguments = {}
            else:
                method_name = method_data.get('method')
                sent_arguments = method_data.get('args')

            method_name = method_name.lower()

            if method_name in self.method_names:
                raise ResourceMethodNotFoundError("'" + class_name + "' cannot export method '" + method_name + "'")
            
            # lookup the method and ensure it is exported
            method = getattr(parent, method_name, None)
            if method is None or not callable(method) or not getattr(method, '_is_exported', False):
                raise ResourceMethodNotFoundError("'" + class_name + "' has no exported method '" + method_name + "'")

            method_kwargs = populate_args(method, sent_arguments, env_arguments)
            result.append(method(**method_kwargs))

        if encode:
            result = self.encode(result, env_arguments)

        return result

    def class_call(self, export_name, methods, env_arguments, allowed_method_types=('create', 'lookup', 'execute'), encode=True):
        resource_class = self._get_resource(export_name)

        # call the class (static) method and encode the result
        return self._call(
            resource_class.__name__,
            resource_class,
            methods,
            self.class_access_method_name,
            env_arguments,
            allowed_method_types,
            encode
        )

    def instance_read(self, export_name, methods, instance_args, env_arguments, allowed_method_types=('read',), encode=True):
        resource_class = self._get_resource(export_name)

        # call the instance method and encode the result
        result = self._call(
            resource_class.__name__,
            resource_class(**instance_args), # instantiate the resource
            methods,
            self.access_method_name,
            env_arguments,
            allowed_method_types,
            encode
        )

        return result

    def instance_write(self, export_name, methods, instance_args, env_arguments, allowed_method_types=('read', 'update', 'delete'), encode=True):
        resource_class = self._get_resource(export_name)

        # call the instance method and encode the result
        try:
            result = self._call(
                resource_class.__name__,
                resource_class(**instance_args), # instantiate the resource
                methods,
                self.access_method_name,
                env_arguments,
                allowed_method_types,
                encode=encode
            )
        except Exception as err:
            # pass on any exceptions raised
            raise err
        else:
            commit = None
            # commit changes to the resource
            if self.is_transactional.get('export_name', True):
                commit_method = getattr(instance, self.commit_method_name, None)
                if commit_method is not None:
                    commit_kwargs = populate_args(commit_method, env_arguments)
                    commit = self._encode(commit_method(**commit_kwargs), env_arguments)
                    
        return {
            'result': result,
            'commit': commit
        }

    def create_call(self, export_name, create_method_name, creation_args, methods, env_arguments, allowed_method_types=('read', 'update', 'create'), encode=True):
        resource_class = self._get_resource(export_name)

        instance = self.class_call(
            export_name, {
                'method': create_method_name, 
                'args': creation_args
            },
            env_arguments,
            allowed_method_types=('create',),
            encode=False
        )[0]

        if not isinstance(instance, resource_class):
            raise ResourceMethodNotFoundError("'" + class_name + "' has no creation method '" + method_name + "'")





resource = API().resource
