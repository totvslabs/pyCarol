class CarolApiResponseException(Exception):

    """Custom Exception to handle Exception on Carol API Calls."""

    pass


class InvalidToken(Exception):

    """Custom Exception to handle Invalid token."""

    pass


class MissingInfoCarolException(ValueError):

    """Custom exception to handle Carol missing information on Carol.__init__."""

    pass


class DeprecatedEnvVarException(Exception):

    """Custom exception to handle deprecated environment variables.

    Args:
        old_var: Deprecated environment variable.
        new_var: Environment variable to be replaced.
    """

    def __init__(self, old_var: str, new_var: str):
        msg = f"""
            {old_var} environment var is deprecated.
            Please replace it by {new_var}.
        """
        super().__init__(msg)


class NoServiceAccountException(Exception):

    """Custom exception to handle missing BQ service account."""

    pass
