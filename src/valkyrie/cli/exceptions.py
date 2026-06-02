from pydantic import ValidationError


class TrackerServiceError(Exception):
    """Exception raised for tracker service errors."""

    pass


class BundlerError(Exception):
    """Exception raised for bundler errors."""

    pass


class ContractValidationError(ValueError):
    """Formats a Pydantic ValidationError into user-friendly messages."""

    FORMATTERS: dict[str, str] = {
        "missing": "'{field}' is required but was not provided",
        "literal_error": "'{field}' has an invalid value. {msg}",
    }

    def __init__(self, validation_error: ValidationError, context: str = "") -> None:
        messages: list[str] = []
        for err in validation_error.errors():
            field = ".".join(str(loc) for loc in err["loc"])
            template = self.FORMATTERS.get(err["type"])
            if template:
                messages.append(f"  - {template.format(field=field, msg=err['msg'])}")
            else:
                messages.append(f"  - '{field}': {err['msg']}")

        detail = "\n".join(messages)
        super().__init__(f"{context}\n{detail}" if context else detail)
