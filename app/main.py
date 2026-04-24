import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from xml.etree import ElementTree as ET

import requests
import vobject
from apscheduler.schedulers.blocking import BlockingScheduler

from odoo_connector import OdooRpcConnector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


CAL_DAV_URL = os.environ.get("CAL_DAV_URL", "https://radicale.example.com/user/calendar/")
CAL_DAV_USER = os.environ.get("CAL_DAV_USER", "user")
CAL_DAV_PASS = os.environ.get("CAL_DAV_PASS", "pass")
TZ = os.environ.get("TZ", "UTC")

# Prefer minute-based interval, keep legacy SYNC_INTERVAL_HOURS as fallback.
raw_interval_minutes = os.environ.get("SYNC_INTERVAL_MINUTES")
if raw_interval_minutes is not None:
    SYNC_INTERVAL_MINUTES = max(1, int(raw_interval_minutes))
else:
    legacy_hours = os.environ.get("SYNC_INTERVAL_HOURS")
    SYNC_INTERVAL_MINUTES = max(1, int(legacy_hours) * 60) if legacy_hours is not None else 15

try:
    LOCAL_TZ = ZoneInfo(TZ)
except Exception:
    logger.warning("Invalid TZ '%s', falling back to UTC", TZ)
    LOCAL_TZ = timezone.utc


def _normalize_text(value):
    return (value or "").strip()


def _build_description(odoo_event):
    description = _normalize_text(odoo_event.get("description"))
    meeting_url = _normalize_text(odoo_event.get("videocall_location"))
    if meeting_url and meeting_url not in description:
        description = (description + "\n\nVideocall: " + meeting_url).strip()
    return description


def _to_local_naive(value):
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(LOCAL_TZ).replace(tzinfo=None)


def _event_uid(event_id):
    return f"odoo-calendar-{event_id}"


def _managed_uid_prefix():
    return "odoo-calendar-"


def _odoo_id_from_uid(uid):
    uid = _normalize_text(uid)
    prefix = _managed_uid_prefix()
    if not uid.startswith(prefix):
        return None
    candidate = uid[len(prefix) :]
    return candidate if candidate else None


def _safe_filename(value):
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value)


def _parse_odoo_dt(raw_value):
    if not raw_value:
        return None
    if isinstance(raw_value, datetime):
        return raw_value
    if hasattr(raw_value, "year") and hasattr(raw_value, "month") and hasattr(raw_value, "day"):
        return raw_value

    value = str(raw_value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed if fmt.endswith("%S") else parsed.date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _to_compare_value(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return value


def _extract_vevent(ics_text):
    if not ics_text:
        return None
    try:
        cal = vobject.readOne(ics_text)
        return getattr(cal, "vevent", None)
    except Exception as ex:
        logger.warning("Failed to parse existing ICS entry (%s)", ex)
        return None


def _event_url_from_href(href):
    href = _normalize_text(href)
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    filename = href.split("/")[-1]
    return f"{CAL_DAV_URL.rstrip('/')}/{filename}"


def _caldav_collection_url():
    return f"{CAL_DAV_URL.rstrip('/')}/"


def get_odoo_events():
    try:
        odoo = OdooRpcConnector().get()
        fields = [
            "id",
            "name",
            "start",
            "stop",
            "allday",
            "location",
            "videocall_location",
            "description",
            "write_date",
            "active"
        ]
        domain = [["active", "=", True], ["partner_ids.user_ids", "=", odoo.env.uid]]
        events = odoo.env["calendar.event"].search_read(domain, fields)
        logger.info("Fetched %s events from Odoo.", len(events))
        return events
    except Exception as ex:
        logger.error("Failed to fetch calendar events from Odoo: %s", ex)
        return []


def get_caldav_events():
    headers = {
        "Depth": "1",
        "Content-Type": "application/xml",
    }
    body = """<?xml version=\"1.0\"?>
<d:propfind xmlns:d=\"DAV:\" xmlns:cal=\"urn:ietf:params:xml:ns:caldav\">
  <d:prop>
    <d:getetag />
    <cal:calendar-data />
  </d:prop>
</d:propfind>"""

    logger.info("Fetching CalDAV events from: %s", CAL_DAV_URL)
    resp = requests.request(
        "PROPFIND",
        _caldav_collection_url(),
        headers=headers,
        data=body,
        auth=(CAL_DAV_USER, CAL_DAV_PASS),
        timeout=30,
    )
    resp.raise_for_status()

    ns = {"d": "DAV:", "cal": "urn:ietf:params:xml:ns:caldav"}
    tree = ET.fromstring(resp.content)

    events = []
    empty_calendar_data_count = 0
    for response_el in tree.findall("d:response", ns):
        href_el = response_el.find("d:href", ns)
        calendar_data_el = response_el.find(".//cal:calendar-data", ns)
        if href_el is None:
            continue
        if calendar_data_el is None or not _normalize_text(calendar_data_el.text):
            empty_calendar_data_count += 1
            continue
        vevent = _extract_vevent(calendar_data_el.text)
        if vevent is None:
            continue
        uid = getattr(getattr(vevent, "uid", None), "value", None)
        x_odoo_id = None
        x_odoo_write_date = None
        for key, values in vevent.contents.items():
            if not values:
                continue
            lowered = key.lower()
            if lowered == "x-odoo-id":
                x_odoo_id = values[0].value
            elif lowered == "x-odoo-write-date":
                x_odoo_write_date = values[0].value
        events.append(
            {
                "href": href_el.text,
                "ics": calendar_data_el.text,
                "vevent": vevent,
                "uid": uid,
                "x_odoo_id": _normalize_text(x_odoo_id),
                "x_odoo_write_date": _normalize_text(x_odoo_write_date),
            }
        )

    if events:
        logger.info("Fetched %s CalDAV events.", len(events))
        return events

    # Some CalDAV servers answer PROPFIND with calendar-data 404/empty; retry via REPORT.
    if empty_calendar_data_count:
        logger.info("PROPFIND returned empty calendar-data for %s resources, retrying with REPORT.", empty_calendar_data_count)

    report_headers = {
        "Depth": "1",
        "Content-Type": "application/xml; charset=utf-8",
    }
    report_body = """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<C:calendar-query xmlns:D=\"DAV:\" xmlns:C=\"urn:ietf:params:xml:ns:caldav\">
  <D:prop>
    <D:getetag />
    <C:calendar-data />
  </D:prop>
  <C:filter>
    <C:comp-filter name=\"VCALENDAR\">
      <C:comp-filter name=\"VEVENT\" />
    </C:comp-filter>
  </C:filter>
</C:calendar-query>"""

    report_resp = requests.request(
        "REPORT",
        _caldav_collection_url(),
        headers=report_headers,
        data=report_body,
        auth=(CAL_DAV_USER, CAL_DAV_PASS),
        timeout=30,
    )
    report_resp.raise_for_status()
    report_tree = ET.fromstring(report_resp.content)

    for response_el in report_tree.findall("d:response", ns):
        href_el = response_el.find("d:href", ns)
        calendar_data_el = response_el.find(".//cal:calendar-data", ns)
        if href_el is None or calendar_data_el is None or not _normalize_text(calendar_data_el.text):
            continue
        vevent = _extract_vevent(calendar_data_el.text)
        if vevent is None:
            continue
        uid = getattr(getattr(vevent, "uid", None), "value", None)
        x_odoo_id = None
        x_odoo_write_date = None
        for key, values in vevent.contents.items():
            if not values:
                continue
            lowered = key.lower()
            if lowered == "x-odoo-id":
                x_odoo_id = values[0].value
            elif lowered == "x-odoo-write-date":
                x_odoo_write_date = values[0].value
        events.append(
            {
                "href": href_el.text,
                "ics": calendar_data_el.text,
                "vevent": vevent,
                "uid": uid,
                "x_odoo_id": _normalize_text(x_odoo_id),
                "x_odoo_write_date": _normalize_text(x_odoo_write_date),
            }
        )

    logger.info("Fetched %s CalDAV events.", len(events))
    return events


def find_existing_event(caldav_events, odoo_event):
    target_uid = _event_uid(odoo_event["id"])
    target_id = str(odoo_event["id"])

    for event in caldav_events:
        if event.get("uid") == target_uid:
            return event
        if event.get("x_odoo_id") == target_id:
            return event
    return None


def _is_managed_caldav_event(caldav_event):
    uid = _normalize_text(caldav_event.get("uid"))
    if uid.startswith(_managed_uid_prefix()):
        return True
    return bool(_normalize_text(caldav_event.get("x_odoo_id")))


def _caldav_event_odoo_id(caldav_event):
    x_odoo_id = _normalize_text(caldav_event.get("x_odoo_id"))
    if x_odoo_id:
        return x_odoo_id
    return _odoo_id_from_uid(caldav_event.get("uid"))


def delete_caldav_event(caldav_event):
    url = _event_url_from_href(caldav_event.get("href"))
    if not url:
        logger.warning("Skipping delete, invalid href for CalDAV event: %s", caldav_event.get("href"))
        return False

    logger.info("Deleting stale CalDAV event at %s", url)
    resp = requests.delete(
        url,
        auth=(CAL_DAV_USER, CAL_DAV_PASS),
        timeout=30,
    )
    if resp.status_code == 404:
        logger.info("CalDAV event already deleted at %s", url)
        return True
    if not resp.ok:
        logger.error("CalDAV delete failed for %s: %s %s", url, resp.status_code, resp.text)
    resp.raise_for_status()
    return True


def event_needs_update(odoo_event, caldav_event):
    vevent = caldav_event["vevent"]

    summary = _normalize_text(getattr(getattr(vevent, "summary", None), "value", ""))
    description = _normalize_text(getattr(getattr(vevent, "description", None), "value", ""))
    location = _normalize_text(getattr(getattr(vevent, "location", None), "value", ""))
    meeting_url = _normalize_text(getattr(getattr(vevent, "url", None), "value", ""))

    existing_start = _to_compare_value(getattr(getattr(vevent, "dtstart", None), "value", None))
    existing_end = _to_compare_value(getattr(getattr(vevent, "dtend", None), "value", None))

    odoo_start = _to_local_naive(_parse_odoo_dt(odoo_event.get("start")))
    odoo_end = _to_local_naive(_parse_odoo_dt(odoo_event.get("stop")))
    odoo_description = _build_description(odoo_event)
    odoo_meeting_url = _normalize_text(odoo_event.get("videocall_location"))
    odoo_write_date = _normalize_text(odoo_event.get("write_date"))
    existing_write_date = _normalize_text(caldav_event.get("x_odoo_write_date"))

    if odoo_write_date and existing_write_date and odoo_write_date != existing_write_date:
        return True

    if odoo_event.get("allday"):
        if isinstance(odoo_start, datetime):
            odoo_start = odoo_start.date()
        if isinstance(odoo_end, datetime):
            odoo_end = odoo_end.date()

    if summary != _normalize_text(odoo_event.get("name")):
        return True
    if description != odoo_description:
        return True
    if location != _normalize_text(odoo_event.get("location")):
        return True
    if meeting_url != odoo_meeting_url:
        return True
    if existing_start != _to_compare_value(odoo_start):
        return True
    if existing_end != _to_compare_value(odoo_end):
        return True

    return False


def _normalize_for_vobject(value):
    """vobject handles naive datetimes more reliably than timezone.utc tzinfo objects."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def event_to_ics(odoo_event):
    uid = _event_uid(odoo_event["id"])
    name = _normalize_text(odoo_event.get("name"))
    start = _to_local_naive(_parse_odoo_dt(odoo_event.get("start")))
    stop = _to_local_naive(_parse_odoo_dt(odoo_event.get("stop")))
    meeting_url = _normalize_text(odoo_event.get("videocall_location"))
    description = _build_description(odoo_event)

    start = _normalize_for_vobject(start)
    stop = _normalize_for_vobject(stop)

    if not name or start is None or stop is None:
        raise ValueError("Odoo event must have name, start and stop values")

    cal = vobject.iCalendar()
    cal.add("prodid").value = "-//Odoo Calendar Sync//EN"
    cal.add("version").value = "2.0"

    vevent = cal.add("vevent")
    vevent.add("uid").value = uid
    vevent.add("summary").value = name

    allday = bool(odoo_event.get("allday"))
    if allday:
        if isinstance(start, datetime):
            start = start.date()
        if isinstance(stop, datetime):
            stop = stop.date()

    vevent.add("dtstart").value = start
    vevent.add("dtend").value = stop

    if _normalize_text(odoo_event.get("location")):
        vevent.add("location").value = _normalize_text(odoo_event.get("location"))
    if meeting_url:
        vevent.add("url").value = meeting_url
    if description:
        vevent.add("description").value = description

    x_odoo_id = vevent.add("x-odoo-id")
    x_odoo_id.value = str(odoo_event["id"])

    odoo_write_date = _normalize_text(odoo_event.get("write_date"))
    if odoo_write_date:
        x_odoo_write_date = vevent.add("x-odoo-write-date")
        x_odoo_write_date.value = odoo_write_date

    timestamp = _normalize_for_vobject(datetime.now(timezone.utc))
    vevent.add("dtstamp").value = timestamp

    return cal.serialize(), uid


def create_or_update_event(odoo_event, existing_event=None):
    try:
        ics_payload, uid = event_to_ics(odoo_event)
    except Exception as ex:
        logger.error("Skipping event id=%s due to invalid ICS data: %s", odoo_event.get("id"), ex)
        return

    headers = {"Content-Type": "text/calendar; charset=utf-8"}

    if existing_event:
        filename = existing_event["href"].split("/")[-1]
        url = f"{CAL_DAV_URL.rstrip('/')}/{filename}"
        logger.info("Updating CalDAV event id=%s at %s", odoo_event["id"], url)
    else:
        filename = f"{_safe_filename(uid)}.ics"
        url = f"{CAL_DAV_URL.rstrip('/')}/{filename}"
        logger.info("Creating CalDAV event id=%s at %s", odoo_event["id"], url)

    resp = requests.put(
        url,
        data=ics_payload.encode("utf-8"),
        auth=(CAL_DAV_USER, CAL_DAV_PASS),
        headers=headers,
        timeout=30,
    )
    if not resp.ok:
        logger.error("CalDAV write failed for event id=%s: %s %s", odoo_event.get("id"), resp.status_code, resp.text)
    resp.raise_for_status()


def sync_calendar_events():
    odoo_events = get_odoo_events()
    caldav_events = get_caldav_events()

    odoo_ids = {str(event.get("id")) for event in odoo_events if event.get("id") is not None}
    created_count = 0
    updated_count = 0
    deleted_count = 0

    for event in odoo_events:
        existing_event = find_existing_event(caldav_events, event)
        try:
            if existing_event:
                if event_needs_update(event, existing_event):
                    create_or_update_event(event, existing_event)
                    updated_count += 1
                else:
                    logger.info("No update needed for event id=%s", event["id"])
            else:
                create_or_update_event(event, None)
                created_count += 1
        except Exception as ex:
            logger.error("Failed to sync Odoo event id=%s: %s", event.get("id"), ex)

    for caldav_event in caldav_events:
        if not _is_managed_caldav_event(caldav_event):
            continue

        linked_odoo_id = _caldav_event_odoo_id(caldav_event)
        if not linked_odoo_id:
            logger.warning("Skipping managed CalDAV event without identifiable Odoo id: %s", caldav_event.get("href"))
            continue
        if linked_odoo_id in odoo_ids:
            continue

        try:
            if delete_caldav_event(caldav_event):
                deleted_count += 1
        except Exception as ex:
            logger.error("Failed to delete stale CalDAV event href=%s: %s", caldav_event.get("href"), ex)

    logger.info(
        "Calendar sync finished. created=%s updated=%s deleted=%s",
        created_count,
        updated_count,
        deleted_count,
    )


def main():
    logger.info("Starting Odoo-CalDAV calendar sync (TZ=%s)", TZ)
    sync_calendar_events()
    logger.info("Odoo-CalDAV calendar sync finished.")


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TZ)
    main()
    scheduler.add_job(main, "interval", minutes=SYNC_INTERVAL_MINUTES)
    logger.info("Scheduled calendar sync every %s minute(s).", SYNC_INTERVAL_MINUTES)
    scheduler.start()

