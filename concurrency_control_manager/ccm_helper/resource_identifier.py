def get_resource_id(obj) -> str:
    """
    Extract a resource identifier for locking/validation.

    - If `obj` is a string, treat it as the table name (table-level locking).
    - If the object has a `table_name` attribute, use that.
    - Otherwise, fall back to `resource_key` or string representation.
    """
    if isinstance(obj, str):
        return obj

    table_name = getattr(obj, "table_name", None)
    if table_name:
        return table_name

    resource_key = getattr(obj, "resource_key", None)
    if resource_key:
        return resource_key

    return str(obj)
