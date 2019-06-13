def int_to_bytes(i: int) -> bytes:
    """
    Wrapper around int.to_bytes. Set some defaults parameters

    """
    return i.to_bytes(i.bit_length() // 8 + 1, 'little', signed=True)


def flat_list(tree: list) -> list:
    """
    Recursively unnest a nested list
    Args:
        tree: nested lists of unlimited depth

    Returns:
        l: flat list

    """
    l = []
    for node in tree:
        if isinstance(node, list):
            l += flat_list(node)
        else:
            l.append(node)
    return l
