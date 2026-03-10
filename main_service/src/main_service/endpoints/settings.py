import requests
import os
import json
from functools import lru_cache
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from main_service import (
    DB,
    PROJECT_ID,
    CLUSTER_ID_TOKEN,
    CURRENT_BURLA_VERSION,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
)

router = APIRouter()
BURLA_BACKEND_URL = "https://backend.burla.dev"
STRIPE_API_BASE_URL = "https://api.stripe.com/v1"
STRIPE_SECRET_NAME = os.environ.get("STRIPE_SECRET_KEY_SECRET_NAME", "stripe_secret_key_test")
BILLING_DOC_PATH = ("billing", "billing")


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return default


def _get_authenticated_user_email(request: Request) -> str:
    user_email = request.session.get("X-User-Email")
    if not user_email or not request.session.get("Authorization"):
        raise HTTPException(status_code=401, detail="Not authenticated")
    return str(user_email)


@lru_cache(maxsize=1)
def _get_stripe_secret_key() -> str:
    from google.cloud import secretmanager

    configured_project = os.environ.get("BURLA_BILLING_SECRETS_PROJECT_ID")
    project_candidates = [configured_project, "burla-prod", PROJECT_ID]

    seen = set()
    ordered_projects = []
    for candidate in project_candidates:
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        ordered_projects.append(candidate)

    client = secretmanager.SecretManagerServiceClient()
    for project_id in ordered_projects:
        secret_path = f"projects/{project_id}/secrets/{STRIPE_SECRET_NAME}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": secret_path})
            api_key = response.payload.data.decode("UTF-8").strip()
            if api_key:
                return api_key
        except Exception:
            continue

    raise HTTPException(
        status_code=500,
        detail=f"Stripe secret {STRIPE_SECRET_NAME} is not accessible from configured projects.",
    )


def _get_billing_ref():
    collection_name, doc_name = BILLING_DOC_PATH
    return DB.collection(collection_name).document(doc_name)


def _get_or_create_billing_doc(user_email: str) -> dict:
    billing_ref = _get_billing_ref()
    snapshot = billing_ref.get()
    doc = snapshot.to_dict() if snapshot.exists else None

    if not doc:
        doc = {
            "stripe_customer_id": None,
            "has_payment_method": False,
            "billing_email": user_email,
        }
        billing_ref.set(doc, merge=True)
        return doc

    normalized = {
        "stripe_customer_id": str(doc.get("stripe_customer_id") or "").strip() or None,
        "has_payment_method": _safe_bool(doc.get("has_payment_method"), False),
        "billing_email": str(doc.get("billing_email") or user_email),
        "default_payment_method_id": str(doc.get("default_payment_method_id") or "").strip() or None,
    }

    patch = {}
    for key, value in normalized.items():
        if doc.get(key) != value:
            patch[key] = value
    if patch:
        billing_ref.set(patch, merge=True)

    return normalized


def _stripe_post(path: str, api_key: str, data):
    response = requests.post(
        f"{STRIPE_API_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {api_key}"},
        data=data,
        timeout=15,
    )

    if response.status_code >= 400:
        error_message = "Stripe API request failed."
        try:
            payload = response.json() or {}
            if isinstance(payload.get("error"), dict):
                error_message = payload["error"].get("message") or error_message
        except Exception:
            pass
        raise HTTPException(status_code=502, detail=error_message)

    return response.json() or {}


def _stripe_get(path: str, api_key: str, params=None):
    response = requests.get(
        f"{STRIPE_API_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {api_key}"},
        params=params,
        timeout=15,
    )

    if response.status_code >= 400:
        error_message = "Stripe API request failed."
        try:
            payload = response.json() or {}
            if isinstance(payload.get("error"), dict):
                error_message = payload["error"].get("message") or error_message
        except Exception:
            pass
        raise HTTPException(status_code=502, detail=error_message)

    return response.json() or {}


def _extract_stripe_id(value) -> str:
    if isinstance(value, dict):
        return str(value.get("id") or "").strip()
    return str(value or "").strip()


def _get_setup_intent_payment_method_id(setup_intent_id: str, stripe_secret_key: str) -> str:
    setup_intent_id = str(setup_intent_id or "").strip()
    if not setup_intent_id:
        return ""

    setup_intent = _stripe_get(f"/setup_intents/{setup_intent_id}", stripe_secret_key)
    return _extract_stripe_id(setup_intent.get("payment_method"))


def _get_customer_email_and_default_payment_method(
    stripe_customer_id: str, stripe_secret_key: str
) -> tuple[str, str]:
    customer = _stripe_get(f"/customers/{stripe_customer_id}", stripe_secret_key)
    customer_email = str(customer.get("email") or "").strip()
    customer_default_payment_method_id = _extract_stripe_id(
        (customer.get("invoice_settings") or {}).get("default_payment_method")
    )
    return customer_email, customer_default_payment_method_id


def _find_any_card_payment_method_id(stripe_customer_id: str, stripe_secret_key: str) -> str:
    payment_methods = _stripe_get(
        "/payment_methods",
        stripe_secret_key,
        params={"customer": stripe_customer_id, "type": "card", "limit": 1},
    )
    data = payment_methods.get("data")
    if not isinstance(data, list) or not data:
        return ""

    return _extract_stripe_id(data[0])


def _set_customer_default_payment_method(
    stripe_customer_id: str, stripe_secret_key: str, payment_method_id: str
) -> None:
    payment_method_id = str(payment_method_id or "").strip()
    if not payment_method_id:
        return

    _stripe_post(
        f"/customers/{stripe_customer_id}",
        stripe_secret_key,
        data=[("invoice_settings[default_payment_method]", payment_method_id)],
    )


def _sync_billing_doc_with_payment_method(
    stripe_customer_id: str,
    stripe_secret_key: str,
    candidate_payment_method_id: str = "",
    candidate_billing_email: Optional[str] = None,
) -> dict:
    stripe_customer_id = str(stripe_customer_id or "").strip()
    if not stripe_customer_id:
        return {
            "stripe_customer_id": None,
            "has_payment_method": False,
            "billing_email": str(candidate_billing_email or "").strip() or None,
            "default_payment_method_id": None,
        }

    billing_ref = _get_billing_ref()
    existing_billing = billing_ref.get().to_dict() or {}
    existing_billing_email = str(existing_billing.get("billing_email") or "").strip()

    customer_email, customer_default_payment_method_id = _get_customer_email_and_default_payment_method(
        stripe_customer_id, stripe_secret_key
    )

    payment_method_id = _extract_stripe_id(candidate_payment_method_id)
    if not payment_method_id:
        payment_method_id = customer_default_payment_method_id
    if not payment_method_id:
        payment_method_id = _find_any_card_payment_method_id(stripe_customer_id, stripe_secret_key)

    has_payment_method = bool(payment_method_id)
    if has_payment_method:
        _set_customer_default_payment_method(stripe_customer_id, stripe_secret_key, payment_method_id)

    billing_email = (
        existing_billing_email
        or str(candidate_billing_email or "").strip()
        or customer_email
        or None
    )

    patch = {
        "stripe_customer_id": stripe_customer_id,
        "has_payment_method": has_payment_method,
        "default_payment_method_id": payment_method_id or None,
    }
    if billing_email:
        patch["billing_email"] = billing_email

    billing_ref.set(patch, merge=True)
    return patch


def _disable_cluster_credits():
    config_ref = DB.collection("cluster_config").document("cluster_config")
    config_ref.set({"credits": False}, merge=True)

    if IN_LOCAL_DEV_MODE and isinstance(LOCAL_DEV_CONFIG, dict):
        LOCAL_DEV_CONFIG["credits"] = False


def _get_quota_limit(machine_type: str, gcp_region: str) -> int:
    quota_doc = DB.collection("cluster_quota").document("cluster_quota").get().to_dict() or {}
    region_quota = quota_doc.get(gcp_region, {})
    machine_type_limits = region_quota.get("machine_type_limits", {})
    return _safe_int(machine_type_limits.get(machine_type), 0)


def _quota_exceeded_message(machine_type: str, gcp_region: str, limit: int, requested: int) -> str:
    return (
        "Quota exceeded.\n"
        f"Limit for {machine_type} in {gcp_region} is {limit}.\n"
        f"Requested: {requested}.\n"
        "We're requesting a quota increase from GCP and will follow up shortly."
    )


@router.get("/v1/settings")
def get_settings(request: Request):
    config_doc = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_doc.get().to_dict()

    if IN_LOCAL_DEV_MODE:
        config_dict = LOCAL_DEV_CONFIG

    node = config_dict.get("Nodes", [{}])[0]
    container = node.get("containers", [{}])[0]

    url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
    response = requests.get(url, headers={"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"})
    response.raise_for_status()
    user_emails = [user["email"] for user in response.json()["authorized_users"]]
    billing_doc = _get_billing_ref().get().to_dict() or {}
    has_payment_method = _safe_bool(billing_doc.get("has_payment_method"), False)
    return {
        "containerImage": container.get("image", ""),
        "machineType": node.get("machine_type", ""),
        "gcpRegion": node.get("gcp_region", ""),
        "machineQuantity": node.get("quantity", 1),
        "diskSize": node.get("disk_size_gb", 50),
        "inactivityTimeout": int(node.get("inactivity_shutdown_time_sec", 600) // 60),
        "users": user_emails,
        "burlaVersion": CURRENT_BURLA_VERSION,
        "googleCloudProjectId": PROJECT_ID,
        "hasPaymentMethod": has_payment_method,
    }


@router.post("/v1/settings")
async def update_settings(request: Request):
    request_json = await request.json()
    config_ref = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_ref.get().to_dict()

    # updates Nodes object in cluster_config doc
    nodes = config_dict.get("Nodes", [{}])
    node = nodes[0]
    container = node.get("containers", [{}])[0]

    requested_machine_type = request_json.get("machineType", node.get("machine_type"))
    requested_region = request_json.get("gcpRegion", node.get("gcp_region"))
    current_quantity = _safe_int(node.get("quantity"), 1)
    requested_quantity = _safe_int(
        request_json.get("machineQuantity", node.get("quantity")),
        current_quantity,
    )

    quota_limit = _get_quota_limit(requested_machine_type, requested_region)
    if requested_quantity > quota_limit:
        message = _quota_exceeded_message(
            machine_type=requested_machine_type,
            gcp_region=requested_region,
            limit=quota_limit,
            requested=requested_quantity,
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "quota_exceeded",
                "message": message,
                "machine_type": requested_machine_type,
                "region": requested_region,
                "limit": quota_limit,
                "requested": requested_quantity,
            },
        )

    container.update(
        {
            "image": request_json.get("containerImage", container.get("image")),
        }
    )
    container.pop("python_command", None)
    container.pop("python_version", None)
    node.update(
        {
            "machine_type": requested_machine_type,
            "gcp_region": requested_region,
            "quantity": requested_quantity,
            "disk_size_gb": request_json.get("diskSize", node.get("disk_size_gb")),
            "inactivity_shutdown_time_sec": (
                request_json.get("inactivityTimeout", node.get("inactivity_shutdown_time_sec")) * 60
                if isinstance(request_json.get("inactivityTimeout"), int)
                else node.get("inactivity_shutdown_time_sec")
            ),
        }
    )
    nodes[0]["containers"] = [container]
    config_ref.update({"Nodes": nodes})

    if IN_LOCAL_DEV_MODE:
        LOCAL_DEV_CONFIG["Nodes"] = nodes
        LOCAL_DEV_CONFIG["Nodes"][0]["machine_type"] = "n4-standard-2"
        LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = 1

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    headers = {"Authorization": authorization, "X-User-Email": email}

    # updates users in backend service
    users_url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
    response = requests.get(users_url, headers={"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"})
    response.raise_for_status()
    current_user_emails = [user["email"] for user in response.json()["authorized_users"]]
    new_user_emails = request_json.get("users", [])
    # add new users
    for email in new_user_emails:
        if email not in current_user_emails:
            response = requests.post(users_url, json={"new_user": email}, headers=headers)
            response.raise_for_status()
    # remove users
    for email in current_user_emails:
        if email not in new_user_emails:
            response = requests.delete(users_url, json={"user_to_remove": email}, headers=headers)
            response.raise_for_status()


@router.post("/v1/settings/credits/disable")
def disable_credits(request: Request):
    if not request.session.get("Authorization") or not request.session.get("X-User-Email"):
        raise HTTPException(status_code=401, detail="Not authenticated")

    _disable_cluster_credits()

    return {"credits": False}


@router.post("/billing/setup-session")
@router.post("/v1/billing/setup-session")
def create_billing_setup_session(request: Request):
    user_email = _get_authenticated_user_email(request)

    billing_ref = _get_billing_ref()
    billing_doc = _get_or_create_billing_doc(user_email)
    stripe_customer_id = billing_doc.get("stripe_customer_id")
    billing_email = billing_doc.get("billing_email") or user_email

    stripe_secret_key = _get_stripe_secret_key()

    if not stripe_customer_id:
        customer = _stripe_post(
            "/customers",
            stripe_secret_key,
            data={
                "email": billing_email,
            },
        )
        stripe_customer_id = customer.get("id")
        if not stripe_customer_id:
            raise HTTPException(status_code=502, detail="Stripe customer creation failed.")
        billing_ref.set({"stripe_customer_id": stripe_customer_id}, merge=True)

    origin = request.headers.get("origin") or str(request.base_url).rstrip("/")
    success_url = (
        f"{origin}/settings?section=usage&billing_setup=success"
        "&checkout_session_id={CHECKOUT_SESSION_ID}"
    )
    cancel_url = f"{origin}/settings?section=usage&billing_setup=cancel"

    session = _stripe_post(
        "/checkout/sessions",
        stripe_secret_key,
        data=[
            ("mode", "setup"),
            ("currency", "usd"),
            ("customer", stripe_customer_id),
            ("success_url", success_url),
            ("cancel_url", cancel_url),
            ("payment_method_types[]", "card"),
        ],
    )

    session_url = session.get("url")
    if not session_url:
        raise HTTPException(status_code=502, detail="Stripe setup session did not return a redirect URL.")

    return {"url": session_url}


@router.post("/billing/portal-session")
@router.post("/v1/billing/portal-session")
def create_billing_portal_session(request: Request):
    _get_authenticated_user_email(request)

    billing_doc = _get_billing_ref().get().to_dict() or {}
    stripe_customer_id = str(billing_doc.get("stripe_customer_id") or "").strip()
    if not stripe_customer_id:
        raise HTTPException(status_code=400, detail="No Stripe customer on file.")

    stripe_secret_key = _get_stripe_secret_key()
    origin = request.headers.get("origin") or str(request.base_url).rstrip("/")
    return_url = f"{origin}/settings?section=billing&billing_portal_return=1"

    # Open the portal directly in payment method update flow instead of the
    # full invoices/subscriptions overview.
    portal_session = _stripe_post(
        "/billing_portal/sessions",
        stripe_secret_key,
        data=[
            ("customer", stripe_customer_id),
            ("return_url", return_url),
            ("flow_data[type]", "payment_method_update"),
            ("flow_data[after_completion][type]", "redirect"),
            ("flow_data[after_completion][redirect][return_url]", return_url),
        ],
    )

    portal_url = str(portal_session.get("url") or "").strip()
    if not portal_url:
        raise HTTPException(status_code=502, detail="Stripe portal session did not return a redirect URL.")

    return {"url": portal_url}


@router.get("/billing/portal-session/redirect")
@router.get("/v1/billing/portal-session/redirect")
def redirect_to_billing_portal_session(request: Request):
    payload = create_billing_portal_session(request)
    portal_url = str((payload or {}).get("url") or "").strip()
    if not portal_url:
        raise HTTPException(status_code=502, detail="Stripe portal session did not return a redirect URL.")
    return RedirectResponse(url=portal_url, status_code=303)


@router.post("/billing/webhook")
@router.post("/v1/billing/webhook")
async def stripe_billing_webhook(request: Request):
    raw_body = await request.body()
    try:
        event = json.loads(raw_body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    event_type = str(event.get("type") or "")
    event_obj = ((event.get("data") or {}).get("object") or {})

    stripe_customer_id = event_obj.get("customer")
    if not stripe_customer_id and event_type.startswith("customer."):
        stripe_customer_id = event_obj.get("id")

    if event_type in {
        "checkout.session.completed",
        "checkout.session.async_payment_succeeded",
        "setup_intent.succeeded",
        "payment_method.attached",
    } and stripe_customer_id:
        stripe_secret_key = _get_stripe_secret_key()
        stripe_customer_id = str(stripe_customer_id).strip()

        setup_intent_id = ""
        candidate_payment_method_id = ""

        if event_type == "setup_intent.succeeded":
            setup_intent_id = _extract_stripe_id(event_obj.get("id"))
            candidate_payment_method_id = _extract_stripe_id(event_obj.get("payment_method"))
        elif event_type in {"checkout.session.completed", "checkout.session.async_payment_succeeded"}:
            setup_intent_id = _extract_stripe_id(event_obj.get("setup_intent"))
        elif event_type == "payment_method.attached":
            candidate_payment_method_id = _extract_stripe_id(event_obj.get("id"))

        if not candidate_payment_method_id and setup_intent_id:
            candidate_payment_method_id = _get_setup_intent_payment_method_id(
                setup_intent_id, stripe_secret_key
            )

        billing_doc = _sync_billing_doc_with_payment_method(
            stripe_customer_id=stripe_customer_id,
            stripe_secret_key=stripe_secret_key,
            candidate_payment_method_id=candidate_payment_method_id,
        )
        has_payment_method = _safe_bool(billing_doc.get("has_payment_method"), False)
        if has_payment_method:
            _disable_cluster_credits()

    return {"received": True}


@router.post("/billing/confirm-setup-session")
@router.post("/v1/billing/confirm-setup-session")
async def confirm_billing_setup_session(request: Request):
    user_email = _get_authenticated_user_email(request)
    request_json = await request.json()
    checkout_session_id = str(request_json.get("session_id") or "").strip()
    if not checkout_session_id:
        raise HTTPException(status_code=400, detail="Missing session_id")

    stripe_secret_key = _get_stripe_secret_key()
    session = _stripe_get(f"/checkout/sessions/{checkout_session_id}", stripe_secret_key)
    stripe_customer_id = str(session.get("customer") or "").strip()

    if not stripe_customer_id:
        raise HTTPException(status_code=400, detail="Checkout session has no customer.")

    setup_intent_id = _extract_stripe_id(session.get("setup_intent"))
    candidate_payment_method_id = _extract_stripe_id(session.get("payment_method"))
    if not candidate_payment_method_id and setup_intent_id:
        candidate_payment_method_id = _get_setup_intent_payment_method_id(
            setup_intent_id, stripe_secret_key
        )

    billing_doc = _sync_billing_doc_with_payment_method(
        stripe_customer_id=stripe_customer_id,
        stripe_secret_key=stripe_secret_key,
        candidate_payment_method_id=candidate_payment_method_id,
        candidate_billing_email=user_email,
    )
    has_payment_method = _safe_bool(billing_doc.get("has_payment_method"), False)

    if has_payment_method:
        _disable_cluster_credits()

    return {
        "has_payment_method": has_payment_method,
        "default_payment_method_id": billing_doc.get("default_payment_method_id"),
        "stripe_customer_id": billing_doc.get("stripe_customer_id"),
    }
