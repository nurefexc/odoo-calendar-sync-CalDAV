import logging
import os
from datetime import date, datetime, timedelta, timezone
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


def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name, default, minimum=0):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning("Invalid integer for %s=%r, using default=%s", name, raw, default)
        return default


ENABLE_CALDAV_TO_ODOO = _env_bool("ENABLE_CALDAV_TO_ODOO", True)
IMPORT_UNMANAGED_CALDAV = _env_bool("IMPORT_UNMANAGED_CALDAV", True)
SYNC_DRY_RUN = _env_bool("SYNC_DRY_RUN", False)

MAX_ODOO_TO_CALDAV_CREATE = _env_int("MAX_ODOO_TO_CALDAV_CREATE", 200)
MAX_ODOO_TO_CALDAV_UPDATE = _env_int("MAX_ODOO_TO_CALDAV_UPDATE", 200)
MAX_ODOO_TO_CALDAV_DELETE = _env_int("MAX_ODOO_TO_CALDAV_DELETE", 200)

MAX_CALDAV_TO_ODOO_CREATE = _env_int("MAX_CALDAV_TO_ODOO_CREATE", 50)
MAX_CALDAV_TO_ODOO_UPDATE = _env_int("MAX_CALDAV_TO_ODOO_UPDATE", 200)
MAX_CALDAV_IMPORT_CANDIDATES = _env_int("MAX_CALDAV_IMPORT_CANDIDATES", 50)

ODOO_EVENT_FIELDS = [
    "id",
    "name",
    "start",
    "stop",
    "allday",
    "location",
    "videocall_location",
    "description",
    "write_date",
    "active",
]

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


def _to_utc_naive_datetime(value, assume_local_for_naive=False):
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        if assume_local_for_naive:
            return value.replace(tzinfo=LOCAL_TZ).astimezone(timezone.utc).replace(tzinfo=None)
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_datetime_value(raw_value):
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, date):
        return datetime.combine(raw_value, datetime.min.time())

    text = _normalize_text(str(raw_value))
    if not text:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d":
                return datetime.combine(parsed.date(), datetime.min.time())
            return parsed
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _caldav_last_change_utc(caldav_event):
    vevent = caldav_event["vevent"]
    candidates = [
        _vevent_value(vevent, "last_modified"),
        _vevent_value(vevent, "dtstamp"),
    ]
    for candidate in candidates:
        parsed = _parse_datetime_value(candidate)
        normalized = _to_utc_naive_datetime(parsed, assume_local_for_naive=True)
        if normalized is not None:
            return normalized
    return None


def _odoo_write_date_utc(odoo_event):
    parsed = _parse_datetime_value(odoo_event.get("write_date"))
    # Odoo write_date is UTC stored without tzinfo.
    return _to_utc_naive_datetime(parsed, assume_local_for_naive=False)


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


def _get_odoo():
    return OdooRpcConnector().get()


def get_odoo_events(odoo=None):
    try:
        odoo = odoo or _get_odoo()
        domain = [["active", "=", True], ["partner_ids.user_ids", "=", odoo.env.uid]]
        events = odoo.env["calendar.event"].search_read(domain, ODOO_EVENT_FIELDS)
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


def _vevent_value(vevent, key, default=None):
    return getattr(getattr(vevent, key, None), "value", default)


def _odoo_time_window(odoo_event):
    odoo_start = _to_local_naive(_parse_odoo_dt(odoo_event.get("start")))
    odoo_end = _to_local_naive(_parse_odoo_dt(odoo_event.get("stop")))
    if odoo_event.get("allday"):
        if isinstance(odoo_start, datetime):
            odoo_start = odoo_start.date()
        if isinstance(odoo_end, datetime):
            odoo_end = odoo_end.date()
    return _to_compare_value(odoo_start), _to_compare_value(odoo_end)


def _caldav_time_window(caldav_event):
    vevent = caldav_event["vevent"]
    existing_start = _to_compare_value(_vevent_value(vevent, "dtstart"))
    existing_end = _to_compare_value(_vevent_value(vevent, "dtend"))
    return existing_start, existing_end


def _event_projection_from_odoo(odoo_event):
    odoo_start, odoo_end = _odoo_time_window(odoo_event)
    return {
        "summary": _normalize_text(odoo_event.get("name")),
        "description": _build_description(odoo_event),
        "location": _normalize_text(odoo_event.get("location")),
        "meeting_url": _normalize_text(odoo_event.get("videocall_location")),
        "start": odoo_start,
        "end": odoo_end,
    }


def _event_projection_from_caldav(caldav_event):
    vevent = caldav_event["vevent"]
    existing_start, existing_end = _caldav_time_window(caldav_event)
    return {
        "summary": _normalize_text(_vevent_value(vevent, "summary", "")),
        "description": _normalize_text(_vevent_value(vevent, "description", "")),
        "location": _normalize_text(_vevent_value(vevent, "location", "")),
        "meeting_url": _normalize_text(_vevent_value(vevent, "url", "")),
        "start": existing_start,
        "end": existing_end,
    }


def _events_are_equal(odoo_event, caldav_event):
    return _event_projection_from_odoo(odoo_event) == _event_projection_from_caldav(caldav_event)


def _odoo_should_push_version(odoo_event, caldav_event):
    odoo_write_date = _normalize_text(odoo_event.get("write_date"))
    existing_write_date = _normalize_text(caldav_event.get("x_odoo_write_date"))
    if not existing_write_date:
        return True
    if not (odoo_write_date and odoo_write_date != existing_write_date):
        return False

    odoo_write_at = _odoo_write_date_utc(odoo_event)
    caldav_changed_at = _caldav_last_change_utc(caldav_event)

    # If CalDAV event changed after Odoo write_date, prefer pulling CalDAV into Odoo.
    if odoo_write_at is not None and caldav_changed_at is not None and caldav_changed_at > odoo_write_at:
        return False
    return True


def _odoo_dt_to_string(value):
    if isinstance(value, datetime):
        # Odoo datetime fields are stored as UTC naive values.
        # Floating CalDAV datetimes are interpreted in LOCAL_TZ first.
        if value.tzinfo is None:
            value = value.replace(tzinfo=LOCAL_TZ)
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return ""


def _default_end_for_start(start_value):
    if isinstance(start_value, datetime):
        return start_value + timedelta(hours=1)
    if isinstance(start_value, date):
        return start_value + timedelta(days=1)
    return None


def _caldav_event_to_odoo_payload(caldav_event, odoo_uid):
    vevent = caldav_event["vevent"]
    name = _normalize_text(_vevent_value(vevent, "summary", ""))
    start = _vevent_value(vevent, "dtstart")
    stop = _vevent_value(vevent, "dtend")
    if stop is None:
        stop = _default_end_for_start(start)

    if not name or start is None or stop is None:
        return None

    allday = isinstance(start, date) and not isinstance(start, datetime)
    payload = {
        "name": name,
        "start": _odoo_dt_to_string(start),
        "stop": _odoo_dt_to_string(stop),
        "allday": allday,
        "location": _normalize_text(_vevent_value(vevent, "location", "")) or False,
        "videocall_location": _normalize_text(_vevent_value(vevent, "url", "")) or False,
        "description": _normalize_text(_vevent_value(vevent, "description", "")) or False,
        "active": True,
        "user_id": odoo_uid,
    }
    if not payload["start"] or not payload["stop"]:
        return None
    return payload


def _read_odoo_event(odoo, event_id):
    records = odoo.env["calendar.event"].search_read([["id", "=", int(event_id)]], ODOO_EVENT_FIELDS)
    return records[0] if records else None


def create_or_update_odoo_event(odoo, caldav_event, existing_odoo_event=None):
    payload = _caldav_event_to_odoo_payload(caldav_event, odoo.env.uid)
    if payload is None:
        logger.warning("Skipping CalDAV->Odoo sync due to missing required fields for href=%s", caldav_event.get("href"))
        return None

    model = odoo.env["calendar.event"]
    if existing_odoo_event:
        event_id = int(existing_odoo_event["id"])
        logger.info("Updating Odoo event id=%s from CalDAV href=%s", event_id, caldav_event.get("href"))
        model.browse(event_id).write(payload)
        return _read_odoo_event(odoo, event_id)

    logger.info("Creating Odoo event from CalDAV href=%s", caldav_event.get("href"))
    event_id = model.create(payload)
    return _read_odoo_event(odoo, event_id)


def _event_fingerprint_from_odoo(odoo_event):
    start, end = _odoo_time_window(odoo_event)
    if start is None or end is None:
        return None
    return (
        _normalize_text(odoo_event.get("name")).lower(),
        start,
        end,
        bool(odoo_event.get("allday")),
    )


def _event_fingerprint_from_caldav(caldav_event):
    vevent = caldav_event["vevent"]
    start, end = _caldav_time_window(caldav_event)
    if start is None or end is None:
        return None
    return (
        _normalize_text(_vevent_value(vevent, "summary", "")).lower(),
        start,
        end,
        isinstance(_vevent_value(vevent, "dtstart"), date) and not isinstance(_vevent_value(vevent, "dtstart"), datetime),
    )


def _build_caldav_indexes(caldav_events):
    by_uid = {}
    by_odoo_id = {}
    for event in caldav_events:
        uid = _normalize_text(event.get("uid"))
        odoo_id = _normalize_text(event.get("x_odoo_id"))
        if uid and uid not in by_uid:
            by_uid[uid] = event
        if odoo_id and odoo_id not in by_odoo_id:
            by_odoo_id[odoo_id] = event
    return by_uid, by_odoo_id


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
    return not _events_are_equal(odoo_event, caldav_event) or _odoo_should_push_version(odoo_event, caldav_event)


def _normalize_for_vobject(value):
    """vobject handles naive datetimes more reliably than timezone.utc tzinfo objects."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def event_to_ics(odoo_event, preferred_uid=None):
    uid = _normalize_text(preferred_uid) or _event_uid(odoo_event["id"])
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
        preferred_uid = _normalize_text(existing_event.get("uid")) if existing_event else None
        ics_payload, uid = event_to_ics(odoo_event, preferred_uid=preferred_uid)
    except Exception as ex:
        logger.error("Skipping event id=%s due to invalid ICS data: %s", odoo_event.get("id"), ex)
        return

    headers = {"Content-Type": "text/calendar; charset=utf-8"}

    if existing_event:
        filename = _normalize_text(existing_event.get("href")).split("/")[-1]
        if not filename:
            filename = f"{_safe_filename(uid)}.ics"
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
    odoo = _get_odoo()
    odoo_events = get_odoo_events(odoo)
    caldav_events = get_caldav_events()

    odoo_ids = {str(event.get("id")) for event in odoo_events if event.get("id") is not None}
    odoo_by_id = {str(event["id"]): event for event in odoo_events if event.get("id") is not None}
    odoo_fp = {}
    for event in odoo_events:
        fp = _event_fingerprint_from_odoo(event)
        if fp is not None and fp not in odoo_fp:
            odoo_fp[fp] = event

    uid_index, odoo_id_index = _build_caldav_indexes(caldav_events)
    matched_hrefs = set()

    odoo_to_caldav_created = 0
    odoo_to_caldav_updated = 0
    odoo_to_caldav_deleted = 0
    caldav_to_odoo_created = 0
    caldav_to_odoo_updated = 0

    for event in odoo_events:
        event_id = str(event["id"])
        existing_event = uid_index.get(_event_uid(event_id)) or odoo_id_index.get(event_id)
        if existing_event:
            matched_hrefs.add(_normalize_text(existing_event.get("href")))

        try:
            if not existing_event:
                if odoo_to_caldav_created >= MAX_ODOO_TO_CALDAV_CREATE:
                    logger.warning("Skipping Odoo->CalDAV create due to MAX_ODOO_TO_CALDAV_CREATE=%s", MAX_ODOO_TO_CALDAV_CREATE)
                    continue
                if SYNC_DRY_RUN:
                    logger.info("[DRY-RUN] Would create CalDAV event for Odoo id=%s", event_id)
                else:
                    create_or_update_event(event, None)
                odoo_to_caldav_created += 1
                continue

            if not event_needs_update(event, existing_event):
                logger.info("No update needed for event id=%s", event_id)
                continue

            if _odoo_should_push_version(event, existing_event) or not ENABLE_CALDAV_TO_ODOO:
                if odoo_to_caldav_updated >= MAX_ODOO_TO_CALDAV_UPDATE:
                    logger.warning("Skipping Odoo->CalDAV update due to MAX_ODOO_TO_CALDAV_UPDATE=%s", MAX_ODOO_TO_CALDAV_UPDATE)
                    continue
                if SYNC_DRY_RUN:
                    logger.info("[DRY-RUN] Would update CalDAV event for Odoo id=%s", event_id)
                else:
                    create_or_update_event(event, existing_event)
                odoo_to_caldav_updated += 1
            else:
                if caldav_to_odoo_updated >= MAX_CALDAV_TO_ODOO_UPDATE:
                    logger.warning("Skipping CalDAV->Odoo update due to MAX_CALDAV_TO_ODOO_UPDATE=%s", MAX_CALDAV_TO_ODOO_UPDATE)
                    continue
                if SYNC_DRY_RUN:
                    logger.info("[DRY-RUN] Would update Odoo event id=%s from CalDAV href=%s", event_id, existing_event.get("href"))
                else:
                    updated_odoo_event = create_or_update_odoo_event(odoo, existing_event, existing_odoo_event=event)
                    if updated_odoo_event:
                        odoo_by_id[event_id] = updated_odoo_event
                        updated_fp = _event_fingerprint_from_odoo(updated_odoo_event)
                        if updated_fp is not None:
                            odoo_fp[updated_fp] = updated_odoo_event
                        create_or_update_event(updated_odoo_event, existing_event)
                caldav_to_odoo_updated += 1
        except Exception as ex:
            logger.error("Failed to sync linked event id=%s: %s", event_id, ex)

    unmanaged_candidates = []
    for caldav_event in caldav_events:
        href_key = _normalize_text(caldav_event.get("href"))
        if href_key in matched_hrefs:
            continue

        if _is_managed_caldav_event(caldav_event):
            linked_odoo_id = _caldav_event_odoo_id(caldav_event)
            if linked_odoo_id in odoo_ids:
                continue
            if not linked_odoo_id:
                logger.warning("Skipping managed CalDAV event without identifiable Odoo id: %s", caldav_event.get("href"))
                continue
            try:
                if odoo_to_caldav_deleted >= MAX_ODOO_TO_CALDAV_DELETE:
                    logger.warning("Skipping CalDAV delete due to MAX_ODOO_TO_CALDAV_DELETE=%s", MAX_ODOO_TO_CALDAV_DELETE)
                    continue
                if SYNC_DRY_RUN:
                    logger.info("[DRY-RUN] Would delete stale managed CalDAV event href=%s", caldav_event.get("href"))
                else:
                    if delete_caldav_event(caldav_event):
                        odoo_to_caldav_deleted += 1
                        continue
                odoo_to_caldav_deleted += 1
            except Exception as ex:
                logger.error("Failed to delete stale CalDAV event href=%s: %s", caldav_event.get("href"), ex)
            continue

        if ENABLE_CALDAV_TO_ODOO and IMPORT_UNMANAGED_CALDAV:
            unmanaged_candidates.append(caldav_event)

    if ENABLE_CALDAV_TO_ODOO and IMPORT_UNMANAGED_CALDAV and unmanaged_candidates:
        if len(unmanaged_candidates) > MAX_CALDAV_IMPORT_CANDIDATES:
            logger.error(
                "Skipping unmanaged CalDAV import because candidate count=%s exceeds MAX_CALDAV_IMPORT_CANDIDATES=%s",
                len(unmanaged_candidates),
                MAX_CALDAV_IMPORT_CANDIDATES,
            )
        else:
            for caldav_event in unmanaged_candidates:
                if caldav_to_odoo_created >= MAX_CALDAV_TO_ODOO_CREATE:
                    logger.warning("Skipping CalDAV->Odoo create due to MAX_CALDAV_TO_ODOO_CREATE=%s", MAX_CALDAV_TO_ODOO_CREATE)
                    break

                fp = _event_fingerprint_from_caldav(caldav_event)
                duplicate_odoo = odoo_fp.get(fp) if fp is not None else None

                try:
                    if duplicate_odoo:
                        logger.info(
                            "Linking existing Odoo event id=%s with unmanaged CalDAV href=%s",
                            duplicate_odoo.get("id"),
                            caldav_event.get("href"),
                        )
                        if SYNC_DRY_RUN:
                            logger.info("[DRY-RUN] Would stamp metadata to CalDAV href=%s", caldav_event.get("href"))
                        else:
                            create_or_update_event(duplicate_odoo, caldav_event)
                        continue

                    if SYNC_DRY_RUN:
                        logger.info("[DRY-RUN] Would create Odoo event from unmanaged CalDAV href=%s", caldav_event.get("href"))
                        caldav_to_odoo_created += 1
                        continue

                    created_odoo_event = create_or_update_odoo_event(odoo, caldav_event, existing_odoo_event=None)
                    if not created_odoo_event:
                        continue

                    caldav_to_odoo_created += 1
                    created_id = str(created_odoo_event["id"])
                    odoo_ids.add(created_id)
                    odoo_by_id[created_id] = created_odoo_event
                    created_fp = _event_fingerprint_from_odoo(created_odoo_event)
                    if created_fp is not None:
                        odoo_fp[created_fp] = created_odoo_event

                    # Stamp bridge metadata back to the same CalDAV resource to avoid duplicate imports.
                    create_or_update_event(created_odoo_event, caldav_event)
                except Exception as ex:
                    logger.error("Failed to import CalDAV event href=%s into Odoo: %s", caldav_event.get("href"), ex)

    logger.info(
        "Calendar sync finished. odoo_to_caldav(created=%s updated=%s deleted=%s) caldav_to_odoo(created=%s updated=%s) dry_run=%s",
        odoo_to_caldav_created,
        odoo_to_caldav_updated,
        odoo_to_caldav_deleted,
        caldav_to_odoo_created,
        caldav_to_odoo_updated,
        SYNC_DRY_RUN,
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

