"""
Module with utility functions for ThingsBoard integration.
"""

from dataclasses import dataclass
import re
import logging
from datetime import datetime, timezone

from pyubx2 import CARRSOLN, FIXTYPE, LASTCORRECTIONAGE
from pygnssutils.globals import FIXTYPE_GGA

from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
from tb_device_http import TBHTTPDevice

logger = logging.getLogger(__name__)

CONNECTION_STRING_PATTERN = (
    r"^(?P<scheme>[a-zA-Z]+)://"
    r"(?:(?P<username>[^:@]+)(?::(?P<password>[^@]+))?@)?"
    r"(?P<host>[^:/?@&]+)"
    r"(?::(?P<port>\d+))?"
    r"(?:\?(?P<query>.*))?$"
)


@dataclass
class ConnectionParameters:
    """
    Class to hold ThingsBoard connection parameters.
    """

    scheme: str
    host: str
    port: int
    username: str
    password: str
    params: dict[str, str]

    @classmethod
    def from_connection_string(cls, conn_str: str):
        """
        Create a ConnectionParameters instance from a connection string.

        :param conn_str: Connection string to parse
        :type conn_str: str
        :returns: ConnectionParameters instance
        :rtype: ConnectionParameters
        :raises ValueError: If connection string format is invalid
        """
        match = re.match(CONNECTION_STRING_PATTERN, conn_str)
        if not match:
            raise ValueError(f"Invalid connection string: {conn_str}")

        params = match.groupdict()

        # Parse query parameters into a dictionary
        query_params = cls.parse_query(params.pop("query") or "")

        return cls(
            scheme=params["scheme"],
            host=params["host"],
            port=int(p) if (p := params["port"]) is not None else None,
            username=params["username"],
            password=params["password"],
            params=query_params,
        )

    @staticmethod
    def parse_query(query: str) -> dict[str, str]:
        """
        Parse query string into a dictionary.

        :param query: Query string to parse
        :type query: str
        :returns: Dictionary of parsed query parameters
        :rtype: dict[str, str]
        """
        if not query:
            return {}
        return dict(param.split("=") for param in query.split("&"))

    @staticmethod
    def str_to_bool(value: str) -> bool:
        """
        Convert string to boolean.

        :param value: String value to convert
        :type value: str
        :returns: Boolean representation of the string
        :rtype: bool
        :raises ValueError: If string cannot be converted to boolean
        """
        if isinstance(value, bool):
            return value
        if value.lower() in ("true", "1", "yes", "on"):
            return True
        if value.lower() in ("false", "0", "no", "off"):
            return False
        raise ValueError(f"Cannot convert '{value}' to boolean")


class ThingsBoardMQTTHandler(TBDeviceMqttClient):
    """
    ThingsBoard MQTT client handler for GNSS telemetry.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize ThingsBoard MQTT client handler.

        :param args: Positional arguments passed to parent class
        :param kwargs: Keyword arguments, with TLS parameters extracted
        """
        self.tls = kwargs.pop("tls", False)
        self.ca_certs = kwargs.pop("ca_certs", None)
        self.cert_file = kwargs.pop("cert_file", None)
        self.key_file = kwargs.pop("key_file", None)
        super().__init__(*args, **kwargs)

    def __enter__(self):
        """
        Enter context manager.

        :returns: Self instance
        :rtype: ThingsBoardMQTTHandler
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit context manager.

        :param exc_type: Exception type
        :param exc_value: Exception value
        :param traceback: Traceback object
        """
        self.stop()
        self.disconnect()

    def connect(self, **kwargs):
        """
        Connect to ThingsBoard MQTT server with TLS configuration.

        Override the parent connect method to automatically use the TLS
        configuration set during initialization.

        :param kwargs: Additional keyword arguments to pass to parent connect method
        :type kwargs: dict
        """
        # pylint: disable=arguments-differ

        # Merge initialization TLS settings with any passed kwargs
        # kwargs take precedence over initialization settings
        connect_kwargs = {
            "tls": self.tls,
            "ca_certs": self.ca_certs,
            "cert_file": self.cert_file,
            "key_file": self.key_file,
        }
        connect_kwargs.update(kwargs)

        # Call parent connect method with merged arguments
        return super().connect(**connect_kwargs)

    @classmethod
    def from_connection_string(cls, conn_str: str):
        """
        Create ThingsBoardMQTTHandler instance from connection string.

        :param conn_str: Connection string in format
        :type conn_str: str
        :returns: ThingsBoardMQTTHandler instance
        :rtype: ThingsBoardMQTTHandler
        :raises ValueError: If scheme is invalid for MQTT connection

        .. note::
           Supported query parameters:

           - client_id: MQTT client identifier
           - tls: Enable TLS (true/false)
           - ca_certs: Path to CA certificate file
           - cert_file: Path to client certificate file
           - key_file: Path to client private key file
           - qos: Quality of service (0, 1, or 2), default is 0

        .. note::
           Examples:

           - mqtt://user:password@host:port
           - mqtt://user@host:port
           - mqtt://host:port
           - mqtt://access_token@host:port?client_id=myclient&tls=true
           - mqtt://user:pass@host:port?tls=true&ca_certs=/path/ca.pem&cert_file=/path/cert.pem&key_file=/path/key.pem

        """
        cp = ConnectionParameters.from_connection_string(conn_str)
        if cp.scheme.lower() != "mqtt":
            raise ValueError(f"Invalid scheme for MQTT connection: {cp.scheme}")

        tls = cp.str_to_bool(cp.params.get("tls", False))
        return cls(
            host=cp.host,
            port=cp.port or (1883 if not tls else 8883),
            username=cp.username,
            password=cp.password,
            quality_of_service=int(cp.params.get("qos", 0)),
            client_id=cp.params.get("client_id", ""),
            tls=tls,
            ca_certs=cp.params.get("ca_certs"),
            cert_file=cp.params.get("cert_file"),
            key_file=cp.params.get("key_file"),
        )

    def send_gnss_telemetry(self, msg: object):
        """
        Send GNSS telemetry data to ThingsBoard.

        :param msg: GNSS message object
        :type msg: object
        """
        result = get_tb_telemetry(msg)

        if result is None:
            return

        ts, values = result

        # NOTE: TBDeviceMqttClient.send_telemetry() expects timestamp to be UNIX timestamp
        # in milliseconds
        telemetry = {"ts": int(ts.timestamp() * 1000), "values": values}
        logger.debug(f"MQTT Telemetry: {telemetry}")

        info: TBPublishInfo = self.send_telemetry(telemetry)
        if isinstance(info, TBPublishInfo):
            rc = info.rc()
            if rc != 0:
                error = TBPublishInfo.ERRORS_DESCRIPTION.get(rc, "Unknown")
                logger.error(f"MQTT Publish failed with error: {error}")
            else:
                logger.debug("MQTT Publish succeeded")


class ThingsBoardHTTPHandler(TBHTTPDevice):
    """
    ThingsBoard HTTP client handler for GNSS telemetry.
    """

    def __enter__(self):
        """
        Enter context manager.

        :returns: Self instance
        :rtype: ThingsBoardHTTPHandler
        :raises ConnectionError: If unable to connect to ThingsBoard HTTP server
        """
        if not self.connect():
            raise ConnectionError("Unable to connect to ThingsBoard HTTP server")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit context manager.

        :param exc_type: Exception type
        :param exc_value: Exception value
        :param traceback: Traceback object
        """
        self.disconnect()

    @classmethod
    def from_connection_string(cls, conn_str: str):
        """
        Create ThingsBoardHTTPHandler instance from connection string.

        :param conn_str: Connection string in format 'http://access_token@host:port' or 'http://host:port'
        :type conn_str: str
        :returns: ThingsBoardHTTPHandler instance
        :rtype: ThingsBoardHTTPHandler
        :raises ValueError: If scheme is invalid for HTTP connection
        """
        cp = ConnectionParameters.from_connection_string(conn_str)
        if cp.scheme.lower() != "http":
            raise ValueError(f"Invalid scheme for HTTP connection: {cp.scheme}")

        return cls(
            host=f"http://{cp.host}:{cp.port or 8080}",
            token=cp.username,
            name=cp.params.get("name", None),
            chunk_size=int(cp.params.get("chunk_size", 0)),
        )

    def disconnect(self):
        """
        Disconnect from ThingsBoard HTTP server.
        """
        self.stop_publish_worker()

    def send_gnss_telemetry(self, msg: object):
        """
        Send GNSS telemetry data to ThingsBoard.

        :param msg: GNSS message object
        :type msg: object
        """
        result = get_tb_telemetry(msg)
        if result is None:
            return

        ts, values = result
        logger.debug(f"HTTP Telemetry: {{ts: {ts}, values: {values}}}")

        # NOTE: TBHTTPDevice.send_telemetry() expects timestamp to be datetime object
        self.send_telemetry(values, timestamp=ts, queued=True)


def _getattr(msg: object, attr: str, default=None, func=None):
    """
    Helper function to get attribute value if it exists, with optional processing.

    :param msg: Object to get attribute from
    :type msg: object
    :param attr: Attribute name
    :type attr: str
    :param default: Default value if attribute doesn't exist
    :param func: Optional processing function to apply to attribute value
    :returns: Attribute value, processed value, or default
    """
    # pylint: disable=broad-except

    # Get attribute value if exists, else return default
    try:
        value = getattr(msg, attr)
    except AttributeError:
        return default

    # Apply processing function if provided, else return value
    if callable(func):
        try:
            return func(value)
        except Exception:
            return default
    return value


def get_status(msg: object):
    """
    Extract current navigation status data from NMEA or UBX message.

    :param msg: Parsed NMEA or UBX navigation message
    :type msg: object
    :returns: Dictionary containing navigation status data
    :rtype: dict
    """
    status = {}
    for attr in (
        "lat",
        "lon",
        "alt",
        "sep",
        "numSV",
        "HDOP",
        "hDOP",
        "diffAge",
        "diffStation",
    ):
        if hasattr(msg, attr):
            status[attr] = _getattr(msg, attr)
    if hasattr(msg, "fixType"):
        # UBX fix type
        status["fix"] = _getattr(
            msg, "fixType", func=lambda x: FIXTYPE.get(x, "NO FIX")
        )
    if hasattr(msg, "carrSoln"):
        # UBX carrier solution status
        status["fix"] = _getattr(
            msg, "carrSoln", func=lambda x: CARRSOLN.get(x, "NO FIX")
        )
    if hasattr(msg, "quality"):
        status["fix"] = _getattr(
            msg, "quality", func=lambda x: FIXTYPE_GGA.get(x, "NO FIX")
        )
    if hasattr(msg, "lastCorrectionAge"):
        # UBX last DGPS correction age from UBX-NAV-PVT
        status["diffage"] = _getattr(
            msg, "lastCorrectionAge", func=lambda x: LASTCORRECTIONAGE.get(x, 0)
        )
    if hasattr(msg, "hMSL"):
        # UBX hMSL is in mm -> convert to meters
        # UBX Orthometric height (MSL = Mean Sea Level reference)
        status["alt"] = _getattr(msg, "hMSL", func=lambda x: x / 1000)
    if hasattr(msg, "hMSL") and hasattr(msg, "height"):
        # UBX Geoid separation in meters -> (msg.height - msg.hMSL) / 1000
        status["sep"] = _getattr(msg, "height", func=lambda x: (x - msg.hMSL) / 1000)
    if hasattr(msg, "hAcc") and hasattr(msg, "identity"):
        # UBX hAcc is in mm -> convert to meters
        unit = 1 if msg.identity == "PUBX00" else 1000
        status["hacc"] = _getattr(msg, "hAcc", func=lambda x: x / unit)
    if hasattr(msg, "spd"):
        # NMEA speed over ground in knots -> convert to km/h
        status["speed"] = _getattr(msg, "spd", func=lambda x: x * 1.85200)

    return status


def get_tb_telemetry(msg: object) -> tuple[datetime, dict]:
    """
    Convert GNSS message (e.g. NMEAMessage) to ThingsBoard telemetry format.

    :param msg: Parsed GNSS message object
    :type msg: object
    :returns: Tuple containing timestamp and telemetry values dictionary, or None if no telemetry values available
    :rtype: tuple[datetime, dict] or None

    .. note::
       - Returns None if no telemetry values are available.
       - ``msg`` is expected to be formatted as parsed data, returned by GNSSReader.read()
       - The returned timestamp ``ts`` is a datetime.datetime object in UTC.
    """
    now = datetime.now(timezone.utc)

    # Get timestamp if available
    if hasattr(msg, "time"):
        # NOTE: current date is used if not available in message
        date = msg.date if hasattr(msg, "date") else now.date()

        # Combine NMEA time and date into UTC datetime
        ts = datetime.combine(date, msg.time, tzinfo=timezone.utc)

        # Ensure ts is not later than current time
        ts = min(ts, now)
    else:
        # Use current timestamp if no time info available
        ts = now

    # Get telemetry values
    status = get_status(msg)
    values = {f"{msg.msgID}_{key}": value for key, value in status.items()}

    if len(values) == 0:
        return None

    return ts, values
