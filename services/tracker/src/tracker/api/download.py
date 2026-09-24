"""Shared download-link resolution for object-store-backed artifacts."""

from fastapi import Request
from fastapi.responses import FileResponse

from tracker.local.storage import FilesystemObjectStore
from tracker.runtime.storage import ObjectStore


def local_file_response(
    objects: ObjectStore, key: str, *, filename: str, media_type: str | None = None
) -> FileResponse | None:
    """Serve a local object directly from disk, or None when storage isn't local."""
    if not isinstance(objects, FilesystemObjectStore):
        return None
    return FileResponse(objects.object_location(key), filename=filename, media_type=media_type)


async def resolve_download_url(
    objects: ObjectStore,
    key: str,
    *,
    request: Request,
    route_name: str,
    route_params: dict[str, object],
    query_params: dict[str, str] | None = None,
    expires_in: int,
) -> tuple[str, int]:
    """Return a presigned download URL, or a self-referencing fallback link for local storage."""
    url = await objects.temporary_download_url(key, expires_in=expires_in)
    if url is not None:
        return url, expires_in

    link = request.url_for(route_name, **route_params).include_query_params(
        download="true", **(query_params or {})
    )
    return str(link), 0
