"""Bearer-token protection for the admin and metrics endpoints."""

import secrets
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import SecretStr

_bearer = HTTPBearer(auto_error=False)


def require_token(
    setting: str,
) -> Callable[[Request, HTTPAuthorizationCredentials | None], Awaitable[None]]:
    """A dependency that checks `Authorization: Bearer <token>` against a setting.

    Secure by default: if the token is not configured, the endpoint is
    disabled (404) rather than open.
    """

    async def check(
        request: Request,
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    ) -> None:
        expected: SecretStr | None = getattr(request.app.state.settings, setting)

        if expected is None or not expected.get_secret_value():
            raise HTTPException(status.HTTP_404_NOT_FOUND)

        presented = credentials.credentials if credentials else ""
        # Constant-time comparison: timing must not reveal partial matches.
        if not secrets.compare_digest(presented.encode(), expected.get_secret_value().encode()):
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED, headers={"WWW-Authenticate": "Bearer"}
            )

    return check
