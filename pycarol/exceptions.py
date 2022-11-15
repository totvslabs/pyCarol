class CarolApiResponseException(Exception):

    """Custom Exception to handle Exception on Carol API Calls."""

    pass


class InvalidToken(Exception):

    """Custom Exception to handle Invalid token."""

    pass


class MissingInfoCarolException(ValueError):

    """When some information is missing to Carol.__init__."""

    pass


class DeprecatedEnvVarException(Exception):

    """When some environment variable is deprecated."""

    def __init__(self, old_var: str, new_var: str):
        msg = f"""
            {old_var} environment var is deprecated.
            Please replace it by {new_var}.
        """
        super().__init__(msg)
