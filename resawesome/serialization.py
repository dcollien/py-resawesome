from datetime import datetime

def get_decoder(types={}):
    def decode_default(obj):
        if isinstance(obj, dict) and '__type__' in obj:
            obj_type = obj['__type__']
            obj_val  = obj.get('__value__')

            if obj_type in types:
                obj_type = types[obj_type]

            return convert_arg(obj_type, obj_val)
        else:
            return obj

    return decode_default

def get_encoder(wrap_types=False):
    def encode_default(obj):
        wrapped_type = None
        encoded_object = obj

        if isinstance(obj, datetime):
            wrapped_type = 'datetime'
            encoded_object = date_to_isoformat(obj)
        elif isinstance(obj, types.GeneratorType) or isinstance(obj, set):
            if isinstance(obj, set):
                wrapped_type = 'set'
            encoded_object = list(obj)
        elif isinstance(obj, Exception):
            wrapped_type = 'exception'
            encoded_object = obj.error_dict
        elif isinstance(obj, type):
            wrapped_type = 'type'
            encoded_object = repr(obj)

        if wrap_types and wrapped_type is not None:
            encoded_object = {
                '__type__': wrapped_type,
                '__value__': encoded_object
            }

        return encoded_object

    return encode_default

def json_decode(obj):
    return obj # TODO

def _convert_datetime(value):
    try:
        value = isoformat_to_date(value)
    except ParseError:
        try:
            value = float(value)
        except ValueError:
            value = None
        else:
            value = timestamp_to_datetime(value)
    return value

def _convert_list(value):
    try:
        value = json_decode(value)
    except ValueError:
        value = [value]
    return value

def convert_arg(type_name, value):
    converter = None
    if not isinstance(type_name, basestring):
        converter = type_name
        type_name = type_name.__name__
    elif type_name == 'json':
        converter = json_decode

    if isinstance(value, basestring):
        if type_name == 'list':
            value = _convert_list(value)
        elif type_name == 'tuple':
            value = (value,)
        elif type_name == 'datetime':
            value = _convert_datetime(value)
        elif type_name == 'dict':
            value = json_decode(value)
        elif type_name == 'int':
            value = int(value)
        elif type_name == 'long':
            value = long(value)
        elif type_name == 'float':
            value = float(value)
        elif type_name == 'bool':
            value = (value.lower() == 'true') or (value.lower() == 'yes') or (value.lower() == '1')
        elif type_name == 'json':
            try:
                value = converter(value)
            except ValueError:
                value = value
        elif converter is not None:
            if isinstance(converter, type) and isinstance(value, converter):
                # Converter is a type constructor (class, function, or python type) and
                # value is already an instance of this type
                pass
            else:
                value = converter(value)
    elif isinstance(value, int) or isinstance(value, float):
        if type_name == 'datetime':
            value = timestamp_to_datetime(value)

    if value is not None and type(value).__name__ != type_name and not converter:
        raise TypeError("Expecting <type '" + type_name + "'>")

    return value

