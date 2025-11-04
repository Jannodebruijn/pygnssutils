"""Unit tests for ThingsBoard integration module."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, time, date

from pygnssutils.thingsboard import (
    ThingsBoardMQTTHandler,
    ThingsBoardHTTPHandler,
    ConnectionParameters,
)


class TestConnectionParameters(unittest.TestCase):
    """Test cases for ConnectionParameters class."""

    def test_from_connection_string_mqtt_basic(self):
        """Test parsing basic MQTT connection string."""
        conn_str = "mqtt://user:pass@example.com:1883"
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "mqtt")
        self.assertEqual(cp.host, "example.com")
        self.assertEqual(cp.port, 1883)
        self.assertEqual(cp.username, "user")
        self.assertEqual(cp.password, "pass")
        self.assertEqual(cp.params, {})

    def test_from_connection_string_http_with_params(self):
        """Test parsing HTTP connection string with query parameters."""
        conn_str = "http://token@host:8080?name=device1&chunk_size=1024"
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "http")
        self.assertEqual(cp.host, "host")
        self.assertEqual(cp.port, 8080)
        self.assertEqual(cp.username, "token")
        self.assertIsNone(cp.password)
        self.assertEqual(cp.params, {"name": "device1", "chunk_size": "1024"})

    def test_from_connection_string_mqtt_with_tls_params(self):
        """Test parsing MQTT connection string with TLS parameters."""
        conn_str = (
            "mqtt://token@host:8883?tls=true&ca_certs=/path/ca.pem&client_id=test"
        )
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "mqtt")
        self.assertEqual(cp.host, "host")
        self.assertEqual(cp.port, 8883)
        self.assertEqual(cp.username, "token")
        self.assertIsNone(cp.password)
        self.assertEqual(
            cp.params, {"tls": "true", "ca_certs": "/path/ca.pem", "client_id": "test"}
        )

    def test_from_connection_string_no_auth(self):
        """Test parsing connection string without authentication."""
        conn_str = "mqtt://example.com:1883"
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "mqtt")
        self.assertEqual(cp.host, "example.com")
        self.assertEqual(cp.port, 1883)
        self.assertIsNone(cp.username)
        self.assertIsNone(cp.password)
        self.assertEqual(cp.params, {})

    def test_from_connection_string_username_only(self):
        """Test parsing connection string with username only."""
        conn_str = "mqtt://user@example.com:1883"
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "mqtt")
        self.assertEqual(cp.host, "example.com")
        self.assertEqual(cp.port, 1883)
        self.assertEqual(cp.username, "user")
        self.assertIsNone(cp.password)
        self.assertEqual(cp.params, {})

    def test_from_connection_string_no_port(self):
        """Test parsing connection string without port."""
        conn_str = "mqtt://user@example.com"
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "mqtt")
        self.assertEqual(cp.host, "example.com")
        self.assertIsNone(cp.port)
        self.assertEqual(cp.username, "user")
        self.assertIsNone(cp.password)

    def test_from_connection_string_no_auth_no_port(self):
        """Test parsing connection string without authentication or port."""
        conn_str = "http://example.com"
        cp = ConnectionParameters.from_connection_string(conn_str)

        self.assertEqual(cp.scheme, "http")
        self.assertEqual(cp.host, "example.com")
        self.assertIsNone(cp.port)
        self.assertIsNone(cp.username)
        self.assertIsNone(cp.password)
        self.assertEqual(cp.params, {})

    def test_from_connection_string_invalid(self):
        """Test that invalid connection strings raise ValueError."""
        invalid_strings = [
            "invalid_string",
            "",
            "://user@host",
            "scheme://",
            "mqtt://@host",  # empty username with @
        ]

        for conn_str in invalid_strings:
            with self.assertRaises(ValueError):
                ConnectionParameters.from_connection_string(conn_str)

    def test_parse_query_empty(self):
        """Test parsing empty query string."""
        result = ConnectionParameters.parse_query("")
        self.assertEqual(result, {})

        result = ConnectionParameters.parse_query(None)
        self.assertEqual(result, {})

    def test_parse_query_with_params(self):
        """Test parsing query string with parameters."""
        query = "param1=value1&param2=value2&param3=value3"
        result = ConnectionParameters.parse_query(query)

        expected = {"param1": "value1", "param2": "value2", "param3": "value3"}
        self.assertEqual(result, expected)

    def test_str_to_bool_true_values(self):
        """Test converting string to boolean - true values."""
        true_values = ["true", "True", "TRUE", "1", "yes", "YES", "on", "ON"]

        for value in true_values:
            self.assertTrue(ConnectionParameters.str_to_bool(value))

    def test_str_to_bool_false_values(self):
        """Test converting string to boolean - false values."""
        false_values = ["false", "False", "FALSE", "0", "no", "NO", "off", "OFF"]

        for value in false_values:
            self.assertFalse(ConnectionParameters.str_to_bool(value))

    def test_str_to_bool_boolean_input(self):
        """Test str_to_bool with boolean input."""
        self.assertTrue(ConnectionParameters.str_to_bool(True))
        self.assertFalse(ConnectionParameters.str_to_bool(False))

    def test_str_to_bool_invalid(self):
        """Test str_to_bool with invalid input."""
        with self.assertRaises(ValueError):
            ConnectionParameters.str_to_bool("invalid")


class TestThingsBoardMQTTHandler(unittest.TestCase):
    """Test cases for ThingsBoardMQTTHandler class."""

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    def test_from_connection_string_with_password(self, mock_init):
        """Test creating handler from connection string with user and password."""
        mock_init.return_value = None
        conn_str = "mqtt://user123:pass456@example.com:1884"

        handler = ThingsBoardMQTTHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="example.com",
            port=1884,
            username="user123",
            password="pass456",
            quality_of_service=0,
            client_id="",
            tls=False,
            ca_certs=None,
            cert_file=None,
            key_file=None,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    def test_from_connection_string_with_tls_params(self, mock_init):
        """Test creating handler with TLS parameters."""
        mock_init.return_value = None
        conn_str = "mqtt://token@host:8883?tls=true&ca_certs=/ca.pem&cert_file=/cert.pem&key_file=/key.pem&qos=2&client_id=myclient"

        handler = ThingsBoardMQTTHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="host",
            port=8883,
            username="token",
            password=None,
            quality_of_service=2,
            client_id="myclient",
            tls=True,
            ca_certs="/ca.pem",
            cert_file="/cert.pem",
            key_file="/key.pem",
        )

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    def test_from_connection_string_access_token_only(self, mock_init):
        """Test creating handler from connection string with access token only."""
        mock_init.return_value = None
        conn_str = "mqtt://access_token_123@thingsboard.io:1883"

        handler = ThingsBoardMQTTHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="thingsboard.io",
            port=1883,
            username="access_token_123",
            password=None,
            quality_of_service=0,
            client_id="",
            tls=False,
            ca_certs=None,
            cert_file=None,
            key_file=None,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    def test_from_connection_string_default_port(self, mock_init):
        """Test creating handler from connection string with default port."""
        mock_init.return_value = None
        conn_str = "mqtt://user:pass@localhost"

        handler = ThingsBoardMQTTHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="localhost",
            port=1883,
            username="user",
            password="pass",
            quality_of_service=0,
            client_id="",
            tls=False,
            ca_certs=None,
            cert_file=None,
            key_file=None,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    def test_from_connection_string_no_auth(self, mock_init):
        """Test creating handler from connection string without authentication."""
        mock_init.return_value = None
        conn_str = "mqtt://example.com:1883"

        handler = ThingsBoardMQTTHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="example.com",
            port=1883,
            username=None,
            password=None,
            quality_of_service=0,
            client_id="",
            tls=False,
            ca_certs=None,
            cert_file=None,
            key_file=None,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    def test_from_connection_string_username_only(self, mock_init):
        """Test creating handler from connection string with username only."""
        mock_init.return_value = None
        conn_str = "mqtt://access_token@example.com:1883"

        handler = ThingsBoardMQTTHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="example.com",
            port=1883,
            username="access_token",
            password=None,
            quality_of_service=0,
            client_id="",
            tls=False,
            ca_certs=None,
            cert_file=None,
            key_file=None,
        )

    def test_from_connection_string_invalid_scheme(self):
        """Test that non-MQTT schemes raise ValueError."""
        with self.assertRaises(ValueError) as context:
            ThingsBoardMQTTHandler.from_connection_string("http://user:pass@host:1883")
        self.assertIn("Invalid scheme for MQTT connection", str(context.exception))

    def test_from_connection_string_invalid_format(self):
        """Test that invalid connection strings raise ValueError."""
        invalid_strings = [
            "mqtt://user@",  # no host
            "mqtt://@host:1883",  # no user
            "invalid_string",  # completely invalid
            "",  # empty string
        ]

        for conn_str in invalid_strings:
            with self.assertRaises(ValueError):
                ThingsBoardMQTTHandler.from_connection_string(conn_str)

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.connect")
    def test_connect_with_tls_config(self, mock_connect, mock_init):
        """Test connect method with TLS configuration."""
        mock_init.return_value = None
        handler = ThingsBoardMQTTHandler(
            host="test", tls=True, ca_certs="/ca", cert_file="/cert", key_file="/key"
        )

        handler.connect()

        mock_connect.assert_called_once_with()

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.connect")
    def test_connect_kwargs_override(self, mock_connect, mock_init):
        """Test that connect kwargs override initialization settings."""
        mock_init.return_value = None
        handler = ThingsBoardMQTTHandler(host="test", tls=False, ca_certs="/ca1")

        handler.connect(tls=True, ca_certs="/ca2")

        mock_connect.assert_called_once_with(tls=True, ca_certs="/ca2")

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    @patch("pygnssutils.thingsboard.get_tb_telemetry")
    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.send_telemetry")
    def test_send_gnss_telemetry_success(
        self, mock_send, mock_get_telemetry, mock_init
    ):
        """Test successful GNSS telemetry sending."""
        mock_init.return_value = None

        # get_tb_telemetry returns (timestamp, values) tuple
        mock_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_values = {"lat": 40.7128}
        mock_get_telemetry.return_value = (mock_timestamp, mock_values)

        mock_result = Mock()
        mock_result.rc.return_value = 0
        mock_send.return_value = mock_result

        handler = ThingsBoardMQTTHandler(host="test")
        msg = Mock()

        handler.send_gnss_telemetry(msg)

        mock_get_telemetry.assert_called_once_with(msg)
        # Verify the expected telemetry format with timestamp in milliseconds
        expected_telemetry = {
            "ts": int(mock_timestamp.timestamp() * 1000),
            "values": mock_values,
        }
        mock_send.assert_called_once_with(expected_telemetry)

    @patch("pygnssutils.thingsboard.ThingsBoardMQTTHandler.__init__")
    @patch("pygnssutils.thingsboard.get_tb_telemetry")
    def test_send_gnss_telemetry_none_telemetry(self, mock_get_telemetry, mock_init):
        """Test GNSS telemetry sending when telemetry is None."""
        mock_init.return_value = None
        mock_get_telemetry.return_value = None

        handler = ThingsBoardMQTTHandler(host="test")
        msg = Mock()

        # Should not raise any exception
        handler.send_gnss_telemetry(msg)

        mock_get_telemetry.assert_called_once_with(msg)


class TestThingsBoardHTTPHandler(unittest.TestCase):
    """Test cases for ThingsBoardHTTPHandler class."""

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    def test_from_connection_string_with_port(self, mock_init):
        """Test creating HTTP handler from connection string with port."""
        mock_init.return_value = None
        conn_str = "http://my_access_token@example.com:9090"

        handler = ThingsBoardHTTPHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="http://example.com:9090",
            token="my_access_token",
            name=None,
            chunk_size=0,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    def test_from_connection_string_with_params(self, mock_init):
        """Test creating HTTP handler with query parameters."""
        mock_init.return_value = None
        conn_str = "http://token@host:8080?name=device1&chunk_size=512"

        handler = ThingsBoardHTTPHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="http://host:8080",
            token="token",
            name="device1",
            chunk_size=512,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    def test_from_connection_string_default_port(self, mock_init):
        """Test creating HTTP handler from connection string with default port."""
        mock_init.return_value = None
        conn_str = "http://access_token_123@thingsboard.io"

        handler = ThingsBoardHTTPHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="http://thingsboard.io:8080",
            token="access_token_123",
            name=None,
            chunk_size=0,
        )

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    def test_from_connection_string_no_auth(self, mock_init):
        """Test creating HTTP handler from connection string without authentication."""
        mock_init.return_value = None
        conn_str = "http://example.com:8080"

        handler = ThingsBoardHTTPHandler.from_connection_string(conn_str)

        mock_init.assert_called_once_with(
            host="http://example.com:8080",
            token=None,
            name=None,
            chunk_size=0,
        )

    def test_from_connection_string_invalid_scheme(self):
        """Test that non-HTTP schemes raise ValueError."""
        with self.assertRaises(ValueError) as context:
            ThingsBoardHTTPHandler.from_connection_string("mqtt://token@host:80")
        self.assertIn("Invalid scheme for HTTP connection", str(context.exception))

    def test_from_connection_string_invalid_format(self):
        """Test that invalid HTTP connection strings raise ValueError."""
        invalid_strings = [
            "http://token@",  # no host
            "http://@host:80",  # no token
            "invalid_string",  # completely invalid
            "",  # empty string
        ]

        for conn_str in invalid_strings:
            with self.assertRaises(ValueError):
                ThingsBoardHTTPHandler.from_connection_string(conn_str)

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.connect")
    def test_context_manager_enter_success(self, mock_connect, mock_init):
        """Test HTTP handler context manager enter method success."""
        mock_init.return_value = None
        mock_connect.return_value = True

        handler = ThingsBoardHTTPHandler(host="test", token="token")

        result = handler.__enter__()

        mock_connect.assert_called_once()
        self.assertIs(result, handler)

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.connect")
    def test_context_manager_enter_failure(self, mock_connect, mock_init):
        """Test HTTP handler context manager enter method failure."""
        mock_init.return_value = None
        mock_connect.return_value = False

        handler = ThingsBoardHTTPHandler(host="test", token="token")

        with self.assertRaises(ConnectionError) as context:
            handler.__enter__()
        self.assertIn(
            "Unable to connect to ThingsBoard HTTP server", str(context.exception)
        )

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.stop_publish_worker")
    def test_context_manager_exit(self, mock_stop, mock_init):
        """Test HTTP handler context manager exit method."""
        mock_init.return_value = None
        handler = ThingsBoardHTTPHandler(host="test", token="token")

        handler.__exit__(None, None, None)

        mock_stop.assert_called_once()

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.stop_publish_worker")
    def test_disconnect(self, mock_stop, mock_init):
        """Test HTTP handler disconnect method."""
        mock_init.return_value = None
        handler = ThingsBoardHTTPHandler(host="test", token="token")

        handler.disconnect()

        mock_stop.assert_called_once()

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    @patch("pygnssutils.thingsboard.get_tb_telemetry")
    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.send_telemetry")
    def test_send_gnss_telemetry_success(
        self, mock_send, mock_get_telemetry, mock_init
    ):
        """Test successful GNSS telemetry sending via HTTP."""
        mock_init.return_value = None

        # get_tb_telemetry returns (timestamp, values) tuple
        mock_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_values = {"lat": 40.7128, "lon": -74.0060}
        mock_get_telemetry.return_value = (mock_timestamp, mock_values)

        handler = ThingsBoardHTTPHandler(host="test", token="token")
        msg = Mock()

        handler.send_gnss_telemetry(msg)

        mock_get_telemetry.assert_called_once_with(msg)
        # Verify the expected parameters: values and timestamp as datetime object
        mock_send.assert_called_once_with(
            mock_values, timestamp=mock_timestamp, queued=True
        )

    @patch("pygnssutils.thingsboard.ThingsBoardHTTPHandler.__init__")
    @patch("pygnssutils.thingsboard.get_tb_telemetry")
    def test_send_gnss_telemetry_none_telemetry(self, mock_get_telemetry, mock_init):
        """Test HTTP GNSS telemetry sending when telemetry is None."""
        mock_init.return_value = None
        mock_get_telemetry.return_value = None

        handler = ThingsBoardHTTPHandler(host="test", token="token")
        msg = Mock()

        # Should not raise any exception
        handler.send_gnss_telemetry(msg)

        mock_get_telemetry.assert_called_once_with(msg)


if __name__ == "__main__":
    unittest.main()
