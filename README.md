# nurefexc

**nurefexc** is a robust, production-ready Python synchronization service designed to bridge **Odoo ERP** and **CalDAV** servers (e.g., Radicale, Nextcloud, iCloud). It ensures your Odoo calendar remains in sync with your preferred mobile or desktop calendar clients using a stateless, idempotent approach.

## Key Features

* **Idempotency & Tracking:** Uses custom ICS properties (`X-ODOO-ID` and `X-ODOO-WRITE-DATE`) to track synchronization state without a local database.
* **Smart Description Merging:** Automatically appends Odoo video call locations to the event description if not already present.
* **Timezone Aware:** Fully supports global operations using the `zoneinfo` module and explicit timezone handling.
* **Stale Event Cleanup:** Automatically identifies and removes events from the CalDAV server that have been deleted or archived in Odoo.
* **Protocol Resilience:** Implements a two-step discovery process (PROPFIND with a fallback to CalDAV REPORT) to ensure compatibility with various server implementations.

## Architecture

The service runs an `APScheduler` loop that performs the following steps:
1.  **Fetch Odoo Events:** Retrieves active calendar events assigned to the authenticated user.
2.  **Fetch CalDAV Collection:** Scans the remote calendar for existing managed events.
3.  **Conflict Resolution:** Compares `write_date` and core fields (summary, location, timing) to determine if an update is required.
4.  **Atomic Updates:** Performs `PUT` or `DELETE` operations to reconcile the states.

## Configuration

The application is configured via environment variables, making it ideal for Docker deployments.

| Variable                | Description                                | Default |
|:------------------------|:-------------------------------------------|:--------|
| `CAL_DAV_URL`           | Full URL to the CalDAV calendar collection | -       |
| `CAL_DAV_USER`          | CalDAV username                            | -       |
| `CAL_DAV_PASS`          | CalDAV password                            | -       |
| `TZ`                    | System timezone (e.g., `Europe/Budapest`)  | `UTC`   |
| `SYNC_INTERVAL_MINUTES` | Frequency of synchronization in minutes    | `15`    |
| `ODOO_URL`              | Odoo server base URL                       | -       |
| `ODOO_DB`               | Odoo database name                         | -       |
| `ODOO_USER`             | Odoo login/email                           | -       |
| `ODOO_PASS`             | Odoo password or API key                   | -       |

## Docker Deployment

### Using Docker Run
```bash
docker run -d \
  --name nurefexc \
  -e ODOO_URL=[https://your-odoo.com](https://your-odoo.com) \
  -e ODOO_DB=my_db \
  -e ODOO_USER=admin \
  -e ODOO_PASS=your_pass \
  -e CAL_DAV_URL=[https://dav.server.com/user/calendar/](https://dav.server.com/user/calendar/) \
  -e CAL_DAV_USER=user \
  -e CAL_DAV_PASS=pass \
  -e TZ=Europe/Budapest \
  nurefexc:latest