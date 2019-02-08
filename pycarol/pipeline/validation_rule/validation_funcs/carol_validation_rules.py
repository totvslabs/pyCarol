
def matches(data, pattern):
    raise NotImplementedError
    # TODO


def contains(data, value):
    raise NotImplementedError
    # TODO


def contains_array(data, value_array):
    raise NotImplementedError
    # TODO


def starts_with(data, value):
    raise NotImplementedError
    # TODO


def ends_with(data, value):
    raise NotImplementedError
    # TODO


def equals(data, value):
    if isinstance(value, list):
        return not any(data not in value)
    return not any(data != value)


def is_empty(data, value):
    raise NotImplementedError
    # TODO


def is_email(data, value):
    raise NotImplementedError
    # TODO


def length_compare(data, value):
    raise NotImplementedError
    # TODO


def length_range(data, value):
    raise NotImplementedError
    # TODO


def is_not_number_only(data):
    return all([not d.isdigit() for d in data])


def is_invalid_brazil_state_tax_id(data):
    raise NotImplementedError
    # TODO


def is_invalid_brazil_tax_id(data):
    raise NotImplementedError
    # TODO


def value_compare(data):
    raise NotImplementedError
    # TODO


def lookup(data):
    raise NotImplementedError
    # TODO


def lookup_multiple(data):
    raise NotImplementedError
    # TODO


def lookup_found(data):
    raise NotImplementedError
    # TODO

