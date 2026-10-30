"""
ingestion/live_api_producer.py
==============================
Live Real-Time External API Ingestion Adapter for Smart Parking Analytics.
Course: AI372IA — Stream Processing and Analytics, RV College of Engineering.

Fetches live sensor records from external Real-Time Open Data APIs
(e.g. City of Melbourne On-Street Parking Sensor API, Socrata/Opendatasoft APIs)
using an optional API Key, normalises them to canonical schema v1.0,
and publishes them directly into the Kafka topic `raw-parking-events`.

Usage:
    python -m ingestion.live_api_producer --api-key "YOUR_API_KEY" --interval 5
    python -m ingestion.live_api_producer --limit 50
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from ingestion.producer import ParkingEventProducer

logger = logging.getLogger("ingestion.live_api")

DEFAULT_API_ENDPOINT = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bay-sensors/records"


class LiveAPIParkingProducer:
    """
    Ingests live sensor events from external real-time APIs using an optional API Key
    and streams them into Apache Kafka.
    """

    def __init__(
        self,
        api_url: str = DEFAULT_API_ENDPOINT,
        api_key: Optional[str] = None,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "raw-parking-events",
        poll_interval: float = 5.0,
    ) -> None:
        self.api_url = api_url
        self.api_key = api_key or os.getenv("PARKING_API_KEY")
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.poll_interval = poll_interval
        self._seq = 0

    def fetch_live_records(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Fetch real-time sensor events from the API endpoint."""
        params = {"limit": limit}
        if self.api_key:
            params["apikey"] = self.api_key

        query_str = urllib.parse.urlencode(params)
        full_url = f"{self.api_url}?{query_str}"

        headers = {
            "User-Agent": "SmartParkingAnalytics-RVCE/1.0",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        req = urllib.request.Request(full_url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=8) as response:
                data = json.loads(response.read().decode("utf-8"))
                results = data.get("results", [])
                logger.info("Successfully fetched %d live sensor records from external API", len(results))
                return results
        except Exception as exc:
            logger.error("Failed to query live parking API (%s): %s", full_url, exc)
            return []

    def normalize_record(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Convert external API record to canonical parking event schema v1.0."""
        self._seq += 1
        kerbside_id = str(raw.get("kerbsideid") or raw.get("bay_id") or f"SLOT_{self._seq:04d}")
        zone_num = str(raw.get("zone_number") or "ZONE_CBD")
        status_raw = str(raw.get("status_description") or raw.get("status") or "FREE").upper()
        status = "OCCUPIED" if status_raw in ("PRESENT", "OCCUPIED") else "FREE"

        ts_raw = raw.get("status_timestamp") or raw.get("lastupdated")
        if not ts_raw:
            ts_str = datetime.now(tz=timezone.utc).isoformat()
        else:
            try:
                dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                ts_str = dt.isoformat()
            except Exception:
                ts_str = datetime.now(tz=timezone.utc).isoformat()

        loc = raw.get("location") or {}
        lat = loc.get("lat") or -37.8180
        lon = loc.get("lon") or 144.9671

        event = {
            "event_id": f"LIVE_API_{kerbside_id}_{int(time.time()*1000)}",
            "lot_id": f"LOT_MELBOURNE_{zone_num}",
            "slot_id": f"SLOT_{kerbside_id}",
            "zone_id": f"ZONE_{zone_num}",
            "city": "Melbourne",
            "status": status,
            "event_timestamp": ts_str,
            "ingestion_timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "source": "REALTIME_API",
            "event_sequence": self._seq,
            "schema_version": "1.0",
            "latitude": float(lat),
            "longitude": float(lon),
            "lot_name": f"Melbourne On-Street Zone {zone_num}",
        }
        return event

    def run(self, max_polls: Optional[int] = None, records_per_poll: int = 15) -> None:
        """Poll the live API and publish to Kafka continuously."""
        logger.info(
            "Starting Live API Parking Producer [URL: %s | API Key: %s]",
            self.api_url,
            "Configured (Hidden)" if self.api_key else "None (Anonymous Public Tier)",
        )

        with ParkingEventProducer(bootstrap_servers=self.bootstrap_servers, topic=self.topic) as producer:
            polls = 0
            total_sent = 0
            while True:
                polls += 1
                raw_records = self.fetch_live_records(limit=records_per_poll)
                for raw in raw_records:
                    canonical_event = self.normalize_record(raw)
                    success = producer.send_event(canonical_event)
                    if success:
                        total_sent += 1

                logger.info(
                    "Poll #%d: Streamed %d live events into Kafka (Total Sent: %d)",
                    polls,
                    len(raw_records),
                    total_sent,
                )

                if max_polls and polls >= max_polls:
                    logger.info("Reached maximum requested polls (%d). Stopping.", max_polls)
                    break

                time.sleep(self.poll_interval)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream live parking data from external API with API Key into Kafka"
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="API Key for external real-time data provider (or set PARKING_API_KEY env var)",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default=DEFAULT_API_ENDPOINT,
        help="REST API endpoint returning real-time parking records",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Poll interval in seconds",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=15,
        help="Number of records per poll",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        default=None,
        help="Stop after N polls (default: run indefinitely)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="raw-parking-events",
        help="Destination Kafka topic",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    client = LiveAPIParkingProducer(
        api_url=args.api_url,
        api_key=args.api_key,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        poll_interval=args.interval,
    )
    client.run(max_polls=args.max_polls, records_per_poll=args.limit)


if __name__ == "__main__":
    main()
