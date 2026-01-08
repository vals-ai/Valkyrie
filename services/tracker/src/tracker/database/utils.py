from sqlalchemy.orm import attributes
from sqlmodel import SQLModel


def has_field_changed(target: SQLModel, field_name: str) -> bool:
    """
    Check if a field has changed between the current and previous state.
    """
    history = attributes.get_history(target, field_name)
    old_value = history.deleted[0] if history.deleted else None
    new_value = history.added[0] if history.added else None

    return bool(new_value != old_value)
