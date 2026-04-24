# -*- coding: utf-8 -*-

import logging
import os
import odoorpc

_logger = logging.getLogger(__name__)


class OdooRpcConnector:
    """Handle connections to an Odoo server using environment variables or defaults."""

    def __init__(self):
        self._host = os.environ.get("ODOO_HOST") or os.environ.get("ODOO_URL", "localhost")
        self._port = int(os.environ.get("ODOO_PORT", 8069))
        self._db = os.environ.get("ODOO_DB", "odoo_db_name")
        self._user = os.environ.get("ODOO_USER", "admin")
        self._password = os.environ.get("ODOO_PASSWORD", "admin")
        self._version = os.environ.get("ODOO_VERSION", "14.0")
        self._protocol = "jsonrpc+ssl" if self._port == 443 else "jsonrpc"

    def get(self, print_info: bool = False) -> odoorpc.ODOO:
        """Get a connected odoorpc object."""
        try:
            if print_info:
                print(
                    f"host={self._host} port={self._port} version={self._version} protocol={self._protocol} "
                    f"db={self._db} username={self._user} password={self._password}"
                )
            _logger.info(
                "host=%s port=%s version=%s protocol=%s db=%s username=%s password=%s",
                self._host,
                self._port,
                self._version,
                self._protocol,
                self._db,
                self._user,
                self._password,
            )
            odoo = odoorpc.ODOO(
                host=self._host,
                port=self._port,
                version=self._version,
                protocol=self._protocol,
                timeout=None  # type: ignore
            )
            odoo.login(db=self._db, login=self._user, password=self._password)
            return odoo
        except Exception as ex:
            _logger.exception(ex)
            raise

