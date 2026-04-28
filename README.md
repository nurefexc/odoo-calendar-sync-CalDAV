# Odoo CalDAV Bridge

**Odoo CalDAV Bridge** is a robust, production-ready Python synchronization service designed to bridge **Odoo ERP** and **CalDAV** servers (e.g., Radicale, Nextcloud, iCloud). It ensures your Odoo calendar remains in sync with your preferred mobile or desktop calendar clients using a stateless, idempotent approach.

## Key Features

* **Idempotency & Tracking:** Uses custom ICS properties (`X-ODOO-ID` and `X-ODOO-WRITE-DATE`) to track synchronization state without a local database.
* **Smart Description Merging:** Automatically appends Odoo video call locations to the event description if not already present.
* **Timezone Aware:** Fully supports global operations using the `zoneinfo` module and explicit timezone handling.
* **Two-Way Sync:** Handles Odoo -> CalDAV and CalDAV -> Odoo updates, including events created in Radicale.
* **Stale Event Cleanup:** Automatically identifies and removes bridge-managed CalDAV events that were deleted or archived in Odoo.
* **Safety Guards:** Dry-run support and hard limits prevent accidental mass create/update/delete operations.
* **Protocol Resilience:** Implements a two-step discovery process (PROPFIND with a fallback to CalDAV REPORT) to ensure compatibility with various server implementations.

## Architecture

The service runs an `APScheduler` loop that performs the following steps:
1.  **Fetch Odoo Events:** Retrieves active calendar events assigned to the authenticated user.
2.  **Fetch CalDAV Collection:** Scans the remote calendar for existing managed events.
3.  **Conflict Resolution:** If Odoo `write_date` differs from `X-ODOO-WRITE-DATE`, Odoo wins; otherwise CalDAV changes are pulled back to Odoo.
4.  **Safe Reconciliation:** Performs bounded `PUT`/`DELETE` (CalDAV) and `create`/`write` (Odoo) operations with duplicate checks.

## Configuration

The application is configured via environment variables, making it ideal for Docker deployments.

| Variable                | Description                                | Default |
|:------------------------|:-------------------------------------------|:--------|
| `CAL_DAV_URL`           | Full URL to the CalDAV calendar collection | -       |
| `CAL_DAV_USER`          | CalDAV username                            | -       |
| `CAL_DAV_PASS`          | CalDAV password                            | -       |
| `TZ`                    | System timezone (e.g., `Europe/Budapest`)  | `UTC`   |
| `SYNC_INTERVAL_MINUTES` | Frequency of synchronization in minutes    | `15`    |
| `ENABLE_CALDAV_TO_ODOO` | Enable CalDAV -> Odoo updates/imports     | `true`  |
| `IMPORT_UNMANAGED_CALDAV` | Import unmanaged/new CalDAV events      | `true`  |
| `SYNC_DRY_RUN`          | Plan and log actions without writing data  | `false` |
| `MAX_ODOO_TO_CALDAV_CREATE` | Max Odoo -> CalDAV creates per run    | `200`   |
| `MAX_ODOO_TO_CALDAV_UPDATE` | Max Odoo -> CalDAV updates per run    | `200`   |
| `MAX_ODOO_TO_CALDAV_DELETE` | Max CalDAV deletes per run            | `200`   |
| `MAX_CALDAV_TO_ODOO_CREATE` | Max CalDAV -> Odoo creates per run    | `50`    |
| `MAX_CALDAV_TO_ODOO_UPDATE` | Max CalDAV -> Odoo updates per run    | `200`   |
| `MAX_CALDAV_IMPORT_CANDIDATES` | Hard cap on unmanaged import set    | `50`    |
| `ODOO_URL`              | Odoo server base URL                       | -       |
| `ODOO_DB`               | Odoo database name                         | -       |
| `ODOO_USER`             | Odoo login/email                           | -       |
| `ODOO_PASS`             | Odoo password or API key                   | -       |

## Docker Deployment

### Using Docker Run
```bash
docker run -d \
  --name odoo-caldav-bridge \
  -e ODOO_URL=[https://your-odoo.com](https://your-odoo.com) \
  -e ODOO_DB=my_db \
  -e ODOO_USER=admin \
  -e ODOO_PASS=your_pass \
  -e CAL_DAV_URL=[https://dav.server.com/user/calendar/](https://dav.server.com/user/calendar/) \
  -e CAL_DAV_USER=user \
  -e CAL_DAV_PASS=pass \
  -e TZ=Europe/Budapest \
  odoo-caldav-bridge:latest