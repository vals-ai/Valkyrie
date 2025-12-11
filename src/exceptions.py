class EnvironmentException(Exception):
    """Exception for all environment-related exceptions"""

    _EXCEPTION_MESSAGE: str = "An error occurred while setting up the environment"

    def __init__(self, message: str = _EXCEPTION_MESSAGE):
        self.message = message

    def __str__(self) -> str:
        return self.message
