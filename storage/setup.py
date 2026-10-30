"""
InfluxDB 2.7 setup and initialisation script for Smart Parking Analytics.

Run once during environment provisioning to:
  1. Verify connectivity.
  2. Create the organisation (if absent).
  3. Create the target bucket with infinite retention.
  4. Create a scoped read-only token for Grafana.
  5. Return a Grafana datasource provisioning config.

Course: AI372IA — Smart Parking Occupancy Analytics
Institution: RV College of Engineering, Bengaluru
"""

from __future__ import annotations

import logging
import os
from typing import Any

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.domain.bucket import Bucket
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules
from influxdb_client.domain.organization import Organization
from influxdb_client.domain.authorization import Authorization
from influxdb_client.domain.permission import Permission
from influxdb_client.domain.permission_resource import PermissionResource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Primary setup entry point
# ---------------------------------------------------------------------------

def setup_influxdb(
    url: str,
    token: str,
    org: str,
    bucket_name: str,
) -> dict[str, Any]:
    """Initialise InfluxDB for the Smart Parking Analytics pipeline.

    Steps
    -----
    1. Creates an ``InfluxDBClient`` with the supplied admin *token*.
    2. Verifies connectivity via :func:`verify_influxdb_ready`.
    3. Finds or creates the *org* organisation.
    4. Finds or creates the *bucket_name* bucket (infinite retention).
    5. Creates a scoped read-only authorisation token for Grafana.
    6. Returns a ``setup_status`` dictionary.

    Args:
        url: InfluxDB server URL (e.g. ``http://localhost:8086``).
        token: Admin API token with full permissions.
        org: Organisation name to create or reuse.
        bucket_name: Target bucket name.

    Returns:
        Dictionary with keys:

        * ``connected`` (bool) — server was reachable.
        * ``org_created`` (bool) — organisation was newly created.
        * ``org_id`` (str) — InfluxDB organisation ID.
        * ``bucket_created`` (bool) — bucket was newly created.
        * ``bucket_id`` (str) — InfluxDB bucket ID.
        * ``grafana_token`` (str | None) — read-only token value for Grafana.
        * ``grafana_token_id`` (str | None) — authorisation ID.
        * ``error`` (str | None) — error message if setup partially failed.

    Raises:
        :class:`RuntimeError`: If the server is not reachable or the admin
            token is invalid.
    """
    status: dict[str, Any] = {
        "connected": False,
        "org_created": False,
        "org_id": None,
        "bucket_created": False,
        "bucket_id": None,
        "grafana_token": None,
        "grafana_token_id": None,
        "error": None,
    }

    logger.info("Starting InfluxDB setup — url=%s org=%s bucket=%s", url, org, bucket_name)

    # Step 1 — connectivity
    if not verify_influxdb_ready(url, token):
        msg = f"Cannot reach InfluxDB at {url}. Check the URL and admin token."
        logger.error(msg)
        raise RuntimeError(msg)

    status["connected"] = True
    logger.info("InfluxDB connectivity confirmed")

    client = InfluxDBClient(url=url, token=token, org=org)

    try:
        orgs_api = client.organizations_api()
        buckets_api = client.buckets_api()
        authorizations_api = client.authorizations_api()

        # Step 2 — org
        org_id, org_created = _find_or_create_org(orgs_api, org)
        status["org_id"] = org_id
        status["org_created"] = org_created
        if org_created:
            logger.info("Organisation '%s' created (id=%s)", org, org_id)
        else:
            logger.info("Organisation '%s' already exists (id=%s)", org, org_id)

        # Step 3 — bucket
        bucket_id, bucket_created = _find_or_create_bucket(
            buckets_api, bucket_name, org_id
        )
        status["bucket_id"] = bucket_id
        status["bucket_created"] = bucket_created
        if bucket_created:
            logger.info(
                "Bucket '%s' created with infinite retention (id=%s)",
                bucket_name,
                bucket_id,
            )
        else:
            logger.info("Bucket '%s' already exists (id=%s)", bucket_name, bucket_id)

        # Step 4 — Grafana read-only token
        try:
            grafana_token, token_id = _create_grafana_token(
                authorizations_api, org_id, bucket_id
            )
            status["grafana_token"] = grafana_token
            status["grafana_token_id"] = token_id
            logger.info(
                "Grafana read-only token created (authorisation id=%s)", token_id
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not create Grafana token: %s", exc)
            status["error"] = f"Grafana token creation failed: {exc}"

    except InfluxDBError as exc:
        logger.error("InfluxDB setup error: %s", exc)
        status["error"] = str(exc)
    finally:
        client.close()

    logger.info("InfluxDB setup complete — status=%s", status)
    return status


# ---------------------------------------------------------------------------
# Health verification
# ---------------------------------------------------------------------------

def verify_influxdb_ready(url: str, token: str) -> bool:
    """Ping InfluxDB and confirm the server is healthy.

    Args:
        url: InfluxDB server URL.
        token: Any valid API token.

    Returns:
        ``True`` if the server responds with ``status == "pass"``.
    """
    try:
        with InfluxDBClient(url=url, token=token) as client:
            health = client.health()
            ok = health.status == "pass"
            if ok:
                logger.debug("InfluxDB health check passed — version=%s", health.version)
            else:
                logger.warning("InfluxDB health check returned status=%s", health.status)
            return ok
    except Exception as exc:  # noqa: BLE001
        logger.error("InfluxDB health check failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Grafana datasource provisioning config
# ---------------------------------------------------------------------------

def create_grafana_datasource_config(
    influx_url: str,
    token: str,
    org: str,
    bucket: str,
) -> dict[str, Any]:
    """Generate a Grafana datasource JSON for auto-provisioning.

    The returned dictionary can be serialised to YAML/JSON and placed in
    ``/etc/grafana/provisioning/datasources/`` so Grafana loads the
    InfluxDB datasource automatically on startup.

    Args:
        influx_url: InfluxDB server URL visible from the Grafana host.
        token: Read-only Grafana token (value returned by :func:`setup_influxdb`).
        org: InfluxDB organisation name.
        bucket: Default bucket to query.

    Returns:
        Grafana datasource provisioning dictionary (Flux query language).

    Example::

        import json
        config = create_grafana_datasource_config(
            "http://influxdb:8086", "<token>", "parking", "parking_analytics"
        )
        print(json.dumps(config, indent=2))
    """
    return {
        "apiVersion": 1,
        "datasources": [
            {
                "name": "SmartParking-InfluxDB",
                "type": "influxdb",
                "access": "proxy",
                "url": influx_url,
                "isDefault": True,
                "jsonData": {
                    "version": "Flux",
                    "organization": org,
                    "defaultBucket": bucket,
                    "tlsSkipVerify": True,
                },
                "secureJsonData": {
                    "token": token,
                },
                "editable": False,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_or_create_org(
    orgs_api: Any,
    org_name: str,
) -> tuple[str, bool]:
    """Return *(org_id, created)* for *org_name*, creating it if absent.

    Args:
        orgs_api: InfluxDB ``OrganizationsApi`` instance.
        org_name: Organisation name to look up or create.

    Returns:
        A tuple of (organisation_id, was_created).

    Raises:
        :class:`InfluxDBError`: On API communication failure.
    """
    existing: list[Organization] = orgs_api.find_organizations(org=org_name)
    if existing:
        return existing[0].id, False

    new_org: Organization = orgs_api.create_organization(name=org_name)
    return new_org.id, True


def _find_or_create_bucket(
    buckets_api: Any,
    bucket_name: str,
    org_id: str,
) -> tuple[str, bool]:
    """Return *(bucket_id, created)* for *bucket_name*, creating if absent.

    Buckets are created with **infinite** retention (retention_rules = []).

    Args:
        buckets_api: InfluxDB ``BucketsApi`` instance.
        bucket_name: Bucket name to look up or create.
        org_id: Organisation ID that will own the bucket.

    Returns:
        A tuple of (bucket_id, was_created).

    Raises:
        :class:`InfluxDBError`: On API communication failure.
    """
    existing: list[Bucket] = buckets_api.find_buckets(name=bucket_name).buckets or []
    for b in existing:
        if b.name == bucket_name:
            return b.id, False

    retention_rule = BucketRetentionRules(type="expire", every_seconds=0)
    new_bucket: Bucket = buckets_api.create_bucket(
        bucket_name=bucket_name,
        org_id=org_id,
        retention_rules=[retention_rule],
    )
    return new_bucket.id, True


def _create_grafana_token(
    authorizations_api: Any,
    org_id: str,
    bucket_id: str,
) -> tuple[str, str]:
    """Create a scoped read-only authorisation token for Grafana.

    Permissions granted:
    * ``read`` on the target bucket.

    Args:
        authorizations_api: InfluxDB ``AuthorizationsApi`` instance.
        org_id: Organisation ID.
        bucket_id: Bucket ID to grant read access on.

    Returns:
        A tuple of (token_value, authorisation_id).

    Raises:
        :class:`InfluxDBError`: On API communication failure.
    """
    read_permission = Permission(
        action="read",
        resource=PermissionResource(
            type="buckets",
            id=bucket_id,
            org_id=org_id,
        ),
    )
    auth = Authorization(
        org_id=org_id,
        permissions=[read_permission],
        description="Grafana read-only token — SmartParking",
    )
    created: Authorization = authorizations_api.create_authorization(auth)
    return created.token, created.id


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _main() -> None:
    """CLI wrapper — reads config from environment variables."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    url = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
    token = os.environ["INFLUXDB_TOKEN"]
    org = os.environ["INFLUXDB_ORG"]
    bucket = os.environ.get("INFLUXDB_BUCKET", "parking_analytics")

    status = setup_influxdb(url=url, token=token, org=org, bucket_name=bucket)
    logger.info("Setup status: %s", status)

    if status.get("grafana_token"):
        grafana_cfg = create_grafana_datasource_config(
            influx_url=url,
            token=status["grafana_token"],
            org=org,
            bucket=bucket,
        )
        import json  # noqa: PLC0415
        print("\n--- Grafana Datasource Config ---")
        print(json.dumps(grafana_cfg, indent=2))


if __name__ == "__main__":
    _main()
