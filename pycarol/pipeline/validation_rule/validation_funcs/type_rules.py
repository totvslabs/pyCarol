def validate_string(data):
    return all(isinstance(d, str) for d in data)


def validate_boolean(data):
    return all(isinstance(d, bool) for d in data)


def validate_date(data):
    # TODO
    return True


def validate_long(data):
    return all(isinstance(d, int) for d in data)


def validate_double(data):
    return all(isinstance(d, float) for d in data)


def validate_binary(data):
    # TODO
    return True


def validate_object(data):
    # TODO
    return True


def validate_geopoint(data):
    # TODO
    return True

