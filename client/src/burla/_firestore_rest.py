"""
Async-only firestore REST client for the burla client.

Replaces the pieces of `google-cloud-firestore` we actually use with direct
HTTP calls to `firestore.googleapis.com/v1`. Every doc read / write / query
goes through the `:commit` / `:runQuery` / `documents` REST endpoints.

Auth: uses the existing service-account key stored in CONFIG_PATH (by
`burla login`) to mint a short-lived GCP access token at construction time.
Refreshes the token on expiry or a 401 response.
"""

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from burla import CONFIG_PATH


_SCOPES = ["https://www.googleapis.com/auth/datastore"]
_DATABASE = "burla"

# Firestore timestamps can carry up to nanosecond precision; Python only has
# microsecond. Truncate to avoid fromisoformat() choking.
_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})?$"
)

_OP_MAP = {
    "==": "EQUAL",
    "!=": "NOT_EQUAL",
    "<": "LESS_THAN",
    "<=": "LESS_THAN_OR_EQUAL",
    ">": "GREATER_THAN",
    ">=": "GREATER_THAN_OR_EQUAL",
    "in": "IN",
    "not-in": "NOT_IN",
    "array-contains": "ARRAY_CONTAINS",
    "array-contains-any": "ARRAY_CONTAINS_ANY",
}


class FirestoreRest:
    """
    Minimal async firestore client covering everything burla's client package
    does against firestore: single-doc read/write, auto-id subcollection add,
    and structured queries with `where` filters.

    Construction blocks briefly to mint the first access token; all other
    operations are fully async. Token refreshes happen transparently on
    expiry or 401, guarded by an asyncio.Lock so concurrent requests share
    one refresh.
    """

    def __init__(self, session: aiohttp.ClientSession):
        config = json.loads(CONFIG_PATH.read_text())
        self.project_id = config["project_id"]
        self.database = _DATABASE
        self.session = session
        self._base = (
            f"https://firestore.googleapis.com/v1/projects/{self.project_id}"
            f"/databases/{self.database}/documents"
        )
        self._doc_name_prefix = (
            f"projects/{self.project_id}/databases/{self.database}/documents"
        )

        self._credentials = service_account.Credentials.from_service_account_info(
            config["client_svc_account_key"], scopes=_SCOPES
        )
        # Mint once synchronously so the first request doesn't block on auth.
        # `refresh` uses `requests` under the hood; brief blocking is fine at
        # construction, and every subsequent refresh is offloaded to a thread.
        self._credentials.refresh(Request())
        self._refresh_lock = asyncio.Lock()

    async def _refresh_token(self) -> None:
        async with self._refresh_lock:
            # Another coroutine may have refreshed while we were waiting on the
            # lock; avoid a redundant network round-trip.
            if self._credentials.token and not self._credentials.expired:
                return
            await asyncio.to_thread(self._credentials.refresh, Request())

    async def _auth_headers(self) -> dict[str, str]:
        if self._credentials.expired or not self._credentials.token:
            await self._refresh_token()
        return {"Authorization": f"Bearer {self._credentials.token}"}

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: Optional[dict] = None,
    ) -> Any:
        headers = await self._auth_headers()
        async with self.session.request(method, url, json=json_body, headers=headers) as response:
            if response.status == 401:
                # Token got rejected (clock skew, key rotated, etc.) - force a
                # refresh and retry exactly once.
                await self._refresh_token()
                headers = await self._auth_headers()
                async with self.session.request(
                    method, url, json=json_body, headers=headers
                ) as retry:
                    return await self._read_response(retry)
            return await self._read_response(response)

    @staticmethod
    async def _read_response(response: aiohttp.ClientResponse) -> Any:
        if response.status == 404:
            return None
        if response.status >= 400:
            text = await response.text()
            raise aiohttp.ClientResponseError(
                response.request_info,
                response.history,
                status=response.status,
                message=text,
                headers=response.headers,
            )
        if response.content_length == 0:
            return None
        try:
            return await response.json()
        except aiohttp.ContentTypeError:
            return None

    # ------------------------------------------------------------------
    # Typed-value encode / decode
    # ------------------------------------------------------------------

    @staticmethod
    def _encode(value: Any) -> dict:
        # bool MUST be checked before int because `bool` is a subclass of `int`.
        if isinstance(value, bool):
            return {"booleanValue": value}
        if value is None:
            return {"nullValue": None}
        if isinstance(value, int):
            return {"integerValue": str(value)}
        if isinstance(value, float):
            return {"doubleValue": value}
        if isinstance(value, str):
            return {"stringValue": value}
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return {"timestampValue": value.isoformat()}
        if isinstance(value, bytes):
            import base64

            return {"bytesValue": base64.b64encode(value).decode("ascii")}
        if isinstance(value, list) or isinstance(value, tuple):
            return {
                "arrayValue": {"values": [FirestoreRest._encode(v) for v in value]}
            }
        if isinstance(value, dict):
            return {
                "mapValue": {
                    "fields": {k: FirestoreRest._encode(v) for k, v in value.items()}
                }
            }
        raise TypeError(f"Unsupported firestore value type: {type(value).__name__}")

    @staticmethod
    def _decode(value: dict) -> Any:
        if "nullValue" in value:
            return None
        if "booleanValue" in value:
            return value["booleanValue"]
        if "integerValue" in value:
            return int(value["integerValue"])
        if "doubleValue" in value:
            return value["doubleValue"]
        if "stringValue" in value:
            return value["stringValue"]
        if "timestampValue" in value:
            return FirestoreRest._decode_timestamp(value["timestampValue"])
        if "bytesValue" in value:
            import base64

            return base64.b64decode(value["bytesValue"])
        if "arrayValue" in value:
            return [
                FirestoreRest._decode(v)
                for v in value["arrayValue"].get("values", [])
            ]
        if "mapValue" in value:
            return {
                k: FirestoreRest._decode(v)
                for k, v in value["mapValue"].get("fields", {}).items()
            }
        raise TypeError(f"Unknown firestore value variant: {list(value.keys())}")

    @staticmethod
    def _decode_timestamp(raw: str) -> datetime:
        match = _TS_RE.match(raw)
        if not match:
            # Fall back to a best-effort parse; let fromisoformat complain.
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        base, fractional, tz = match.groups()
        if fractional:
            fractional = fractional[:6].ljust(6, "0")
            base = f"{base}.{fractional}"
        if tz in (None, "Z"):
            tz = "+00:00"
        elif len(tz) == 5:  # e.g. +0000
            tz = f"{tz[:3]}:{tz[3:]}"
        return datetime.fromisoformat(f"{base}{tz}")

    @classmethod
    def _fields_to_dict(cls, fields: dict) -> dict:
        return {key: cls._decode(value) for key, value in fields.items()}

    @classmethod
    def _dict_to_fields(cls, data: dict) -> dict:
        return {key: cls._encode(value) for key, value in data.items()}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def read_doc(self, path: str) -> Optional[dict]:
        """
        GET a single document at `path` (e.g. `"jobs/abc123"`).
        Returns the fields as a plain Python dict, or None if the doc
        does not exist.
        """
        url = f"{self._base}/{path.lstrip('/')}"
        response = await self._request("GET", url)
        if response is None:
            return None
        return self._fields_to_dict(response.get("fields", {}))

    async def write_doc(
        self,
        path: str,
        data: dict,
        *,
        merge: bool = False,
        array_unions: Optional[dict[str, list]] = None,
    ) -> None:
        """
        Write a document at `path`.

        - merge=False (default): replace semantics (SDK `.set(...)`).
        - merge=True: partial update of only the keys in `data`
          (SDK `.update(...)`). Paired with an `updateMask`.
        - array_unions: {field_path: [values_to_append]} for ArrayUnion
          semantics, applied atomically alongside the update.

        Uses the `:commit` endpoint so the three write shapes share one
        codepath.
        """
        doc_name = f"{self._doc_name_prefix}/{path.lstrip('/')}"
        write: dict[str, Any] = {
            "update": {
                "name": doc_name,
                "fields": self._dict_to_fields(data),
            },
        }
        if merge:
            write["updateMask"] = {"fieldPaths": list(data.keys())}
        if array_unions:
            write["updateTransforms"] = [
                {
                    "fieldPath": field,
                    "appendMissingElements": {
                        "values": [self._encode(v) for v in values],
                    },
                }
                for field, values in array_unions.items()
            ]

        url = f"{self._base}:commit"
        await self._request("POST", url, json_body={"writes": [write]})

    async def add_doc(self, collection_path: str, data: dict) -> str:
        """
        Create a document with an auto-generated id in the given collection
        (e.g. `nodes/abc123/logs`). Returns the new doc's relative path
        (e.g. `"nodes/abc123/logs/8f2..."`).
        """
        url = f"{self._base}/{collection_path.lstrip('/').rstrip('/')}"
        body = {"fields": self._dict_to_fields(data)}
        response = await self._request("POST", url, json_body=body)
        if response is None:
            raise RuntimeError(
                f"Firestore add_doc returned no body for {collection_path!r}"
            )
        name = response.get("name", "")
        return name.removeprefix(f"{self._doc_name_prefix}/")

    async def query(
        self,
        collection: str,
        where: Optional[list[tuple[str, str, Any]]] = None,
    ) -> list[dict]:
        """
        Run a structured query against `collection`. `where` is a list of
        `(field_path, op, value)` tuples - multiple filters are ANDed.
        Supported ops: `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `not-in`,
        `array-contains`, `array-contains-any`.

        Returns a list of field-dicts (one per matching doc) in the order
        firestore returns them.
        """
        structured_query: dict[str, Any] = {
            "from": [{"collectionId": collection}],
        }
        if where:
            filters = [
                {
                    "fieldFilter": {
                        "field": {"fieldPath": field},
                        "op": _OP_MAP[op],
                        "value": self._encode(value),
                    }
                }
                for field, op, value in where
            ]
            if len(filters) == 1:
                structured_query["where"] = filters[0]
            else:
                structured_query["where"] = {
                    "compositeFilter": {"op": "AND", "filters": filters}
                }

        url = f"{self._base}:runQuery"
        response = await self._request(
            "POST", url, json_body={"structuredQuery": structured_query}
        )
        if not response:
            return []
        # runQuery returns a JSON array. Rows without a "document" key mean
        # zero results matched (firestore still emits one placeholder entry).
        results = []
        for row in response:
            document = row.get("document") if isinstance(row, dict) else None
            if document is None:
                continue
            results.append(self._fields_to_dict(document.get("fields", {})))
        return results
