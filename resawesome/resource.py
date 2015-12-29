import functools
import collections
import sys

from util import populate_args

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

class ResourceMethodFailedError(Exception):
    pass

class API(object):
    """Defines an API to which resource classes are attached.

    Decorators:
        resource: A decorator to add a class to this API as a named resource

    Methods:
        create: Creates a new resource instance

        read: Reads from resource instance methods

        update: Calls update/mutation methods on a resource instance

        delete: Calls the delete method on a resource instance

        lookup: Reads from resource class methods (static methods)

        execute: Performs updates/mutations by calling class methods (static methods)

    Helpers:
        encode: Transforms an object into a serializable form, encoding any embedded resource
                classes using the access restrictions requied by a given environment
    """

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
        """Create and configure a new API to which resource classes can be attached.

        The format of the decorated resource class is configured through the constructor's args.

        Args:
            module_root (str): The module path prefix to omit from default resource names
                (if names are not provided and default module paths are used)
            commit_method_name (str): The name of the commit method in the attached resource classes
            access_method_name (str): The name of the method used to determine access permissions for
                resource instances, for a given permission level and environment
            class_access_method_name (str): The name of the method used to determine access permissions
                for class methods (static methods), for a given permission level and environment
            serialization_method_name (str): The name of the method used to transform resource classes
                into a serializable object, given a particular access permission level
            access_level_method_name (str): The name of the method used to determine the most restrictive
                access permission for a given environment and resource class
            permission_order (List[str]): The order in which to evaluate permissions, if no access level
                method is defined on a resource class

        """

        self.module_root = module_root
        self.commit_method_name = commit_method_name
        self.access_method_name = access_method_name
        self.class_access_method_name = class_access_method_name
        self.serialization_method_name = serialization_method_name
        self.permission_order = permission_order

        self.method_names = set([commit_method_name, access_method_name, class_access_method_name, serialization_method_name])

        # name -> class lookup for each resource
        self.resource_classes = {}
        # name -> bool lookup, determines if a resource is transactional        
        self.is_transactional = {}

    def resource(self, cls=None, name=None, is_transactional=True):
        """Configurable decorator to apply to resource classes, to add them to this API"""

        if cls is None:
            return functools.partial(self.resource, name=name, is_transactional=is_transactional)

        if name is None:
            if self.module_root is not None:
                name = re.sub(r'^' + self.module_root, '', cls.__module__) + '.' + cls.__name__
            else:
                name = cls.__module__ + '.' + cls.__name__

        cls._IS_RESOURCE = True

        self.resource_classes[name] = cls
        self.is_transactional[name] = is_transactional

        return cls

    def _access_level(self, resource_instance, environment):
        """Determine a resource's most restrictive access permission for a given environment"""

        # determine if the resource instance has a method to provide the 
        # highest level of access this environment can give
        access_level_method = getattr(resource_instance, self.access_level_method_name, None)
        if access_level_method is not None:
            access_level_kwargs = populate_args(access_level_method, environment)
            access_level = access_level_method(**access_level_kwargs)
        else:
            # fall back on calling its access method in the provided
            # permissions order
            access_method = getattr(resource_instance, self.access_method_name, None)
            access_level = None

            if access_method is not None:
                for permission in self.permission_order:
                    access_kwargs = populate_args(access_method, {'permission': permission}, environment)
                    if access_method(**access_kwargs):
                        access_method = permission
                        break

        return access_level

    def encode(self, obj, environment):
        """Encodes an object into a serializable view, based on the environment's access level"""
        def _encode(inner_obj):
            encoded = inner_obj

            # encode resource objects with their specified serializer,
            # according to the highest level of access which the environment arguments allow
            if getattr(inner_obj, '_IS_RESOURCE', False):
                # determine the access level in this environment
                permission = self._access_level(obj, environment)
                # retrieve the serializer
                serializer = getattr(inner_obj, self.serialization_method_name, None)
                
                if serializer is not None:
                    # encode the resource using its serializer with the provided permission
                    serializer_kwargs = populate_args(serializer, {'permission': permission}, environment)
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

    def _get_resource(self, name):
        """Look up a resource class by name"""

        resource_class = self.resource_classes.get(name, None)
        if resource_class is None:
            raise ResourceNotFoundError("'" + name + "' is not defined as an identifiable resource")

        return resource_class

    def _call(self, class_obj, parent, methods, access_method_name, environment, allowed_method_types=None, encode=True):
        """Performs access and permission checking, calls each specified method 
        (its arguments are combined with the provided environment).
        """

        # look up the access method to check for access
        access_method = getattr(parent, access_method_name, None)
        if access_method is None:
            raise ResourceMethodNotFoundError("'" + class_obj.__name__ + "' is missing an access method")

        # check if this is an acceptable method of execution as per allowed_method_types
        if allowed_method_types is not None and method._method_type not in allowed_method_types:
            raise ResourceNotAllowedError("'" + class_obj.__name__ + "' is not allowed to access '" + method_name + "' in this manner")

        # check that access can be granted to call this method
        permission = method._permission
        access_kwargs = populate_args(access_method, {'permission': permission}, environment)
        if not access_method(**access_kwargs):
            raise ResourceAccessDeniedError("'" + class_obj.__name__ + "' has denied access to '" + method_name + "'")

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
                err = ResourceMethodNotFoundError("'" + class_obj.__name__ + "' cannot export method '" + method_name + "'")
                err.method = method_name
                err.args = sent_arguments
                raise err
            
            class_method = getattr(class_obj, method_name, None)
            if isinstance(class_method, property):
                # this "method" is a decorated property
                num_args = len(sent_arguments)


                try:
                    if num_args == 0:
                        result.append(getattr(parent, method_name))
                    elif method_name in sent_arguments:
                        value = sent_arguments[method_name]
                        setattr(parent, method_name, value)
                        result.append(value)
                except Exception as original_err:
                    # pass on the error information
                    trace = sys.exc_info()[2]
                    err = ResourceMethodFailedError("'" + class_obj.__name__ + "' failed to execute '" + method_name + "'")
                    err.method = method_name
                    err.args = sent_arguments
                    err.error = original_err
                    err.trace = trace
                    raise err, None, trace

            elif callable(class_method):
                # this is a callable method
                # lookup the method and ensure it is exported
                method = getattr(parent, method_name, None)
                if method is None or not getattr(method, '_is_exported', False):
                    err = ResourceMethodNotFoundError("'" + class_obj.__name__ + "' has no exported method '" + method_name + "'")
                    err.method = method_name
                    err.args = sent_arguments
                    raise err

                method_kwargs = populate_args(method, sent_arguments, environment)
                
                try:
                    call_result = method(**method_kwargs)
                except Exception as original_err:
                    # pass on the error information
                    trace = sys.exc_info()[2]
                    err = ResourceMethodFailedError("'" + class_obj.__name__ + "' failed to execute '" + method_name + "'")
                    err.method = method_name
                    err.args = sent_arguments
                    err.error = original_err
                    err.trace = trace
                    raise err, None, trace

                result.append(call_result)
            else:
                err = ResourceMethodNotFoundError("'" + class_obj.__name__ + "' has no method or decorated property '" + method_name + "'")
                err.method = method_name
                err.args = sent_arguments
                raise err

        if encode:
            result = self.encode(result, environment)

        return result

    def _commit(self, name, instance, environment, encode=True):
        commit_result = None
        # commit changes to the resource
        if self.is_transactional.get(name, True):
            commit_method = getattr(instance, self.commit_method_name, None)
            if commit_method is not None:
                commit_kwargs = populate_args(commit_method, environment)
                commit_result = commit_result(**commit_kwargs)
                if encode:
                    commit_result = self.encode(commit_result, environment)

        return commit_result

    def _class_call(self, name, methods, environment, allowed_method_types=('lookup', 'execute'), encode=True):
        resource_class = self._get_resource(name)

        # call the class (static) method and encode the result
        return self._call(
            resource_class,
            resource_class,
            methods,
            self.class_access_method_name,
            environment,
            allowed_method_types,
            encode
        )

    # Public Interface

    def lookup(self, name, methods, environment, allowed_method_types=('lookup',), encode=True):
        """Performs a read operation by calling class/static methods on a named resource."""

        return self._class_call(name, methods, environment, allowed_method_types=allowed_method_types, encode=encode)

    def execute(self, name, methods, environment, allowed_method_types=('execute',), encode=True):
        """Performs an update/mutation operation by calling class/static methods on a named resource."""

        return self._class_call(name, methods, environment, allowed_method_types=allowed_method_types, encode=encode)
    
    def create(self, name, create_method_name, creation_args, methods, environment, allowed_method_types=('read', 'update', 'create'), encode=True):
        """Creates an instance of a named resource, with methods to call on the created instance."""

        resource_class = self._get_resource(name)
        create_methods = [{
            'method': create_method_name, 
            'args': creation_args
        }]

        # create an instance
        instance = self._call(
            resource_class,
            resource_class,
            create_methods,
            self.class_access_method_name,
            environment,
            allowed_method_types=('create',),
            encode=False
        )[0]

        if not isinstance(instance, resource_class):
            raise ResourceMethodNotFoundError("'" + class_name + "' has no creation method '" + method_name + "'")

        if methods is not None and len(methods) > 0:
            result = self._call(
                resource_class,
                instance,
                methods,
                self.access_method_name,
                environment,
                tuple(method_type for method_type in allowed_method_types if method_type != 'create'),
                encode=encode
            )

        commit = self._commit(name, instance, environment, encode=encode)

        return {
            'instance': instance,
            'result': result,
            'commit': commit
        }

    def read(self, name, methods, instance_args, environment, allowed_method_types=('read',), encode=True):
        """Performs a read operation by calling methods on an instance of a named resource."""

        resource_class = self._get_resource(name)

        # call the instance method and encode the result
        result = self._call(
            resource_class,
            resource_class(**instance_args), # instantiate the resource
            methods,
            self.access_method_name,
            environment,
            allowed_method_types,
            encode
        )

        return result

    def update(self, name, methods, instance_args, environment, allowed_method_types=('read', 'update', 'delete'), encode=True):
        """Performs an update/mutation operation by calling methods on an instance of a named resource."""

        resource_class = self._get_resource(name)

        instance = resource_class(**instance_args)

        # call the instance method and encode the result
        result = self._call(
            resource_class,
            instance, # instantiate the resource
            methods,
            self.access_method_name,
            environment,
            allowed_method_types,
            encode=encode
        )
                    
        return {
            'result': result,
            'commit': self._commit(name, instance, environment, encode=encode)
        }

    def delete(self, name, method, instance_args, environment, allowed_method_types=('delete',), encode=True):
        """Performs a delete operation by calling an instance method of a named resource."""

        result = self.update(name, [method], instance_args, environment, allowed_method_types=allowed_method_types, encode=encode)
        result['result'] = result['result'][0]
        return result
