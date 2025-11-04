"""
pygnssutils - FOR TESTING ONLY

Threaded TCP socket server test harness.

Sends messages from file to connected clients with updated timestamps.
"""

import argparse
import logging
import random
from datetime import datetime, timezone
from socketserver import StreamRequestHandler, ThreadingTCPServer
import sys
from time import sleep
from pathlib import Path
import threading
import queue

from pynmeagps import NMEAMessage, NMEAReader

logger = logging.getLogger(__name__)


class TestServer(StreamRequestHandler):
    """
    Threaded TCP client connection handler class.
    """

    def handle(self):
        """
        Handle client connection.
        """

        client_info = f"{self.client_address[0]}:{self.client_address[1]}"
        logger.info(f"Client connected: {client_info}")

        # Access server configuration
        input_file = getattr(self.server, "input_file", None)
        block_size_range = getattr(self.server, "block_size_range", (1, 5))

        if input_file and Path(input_file).exists():
            self._handle_file_mode(input_file, block_size_range)
        else:
            logger.error("No valid input file provided. Closing connection.")

    def _handle_file_mode(self, input_file: str, block_size_range: tuple):
        """
        Handle client connection using file input mode with separate producer/consumer threads.

        :param str input_file: Path to input file containing NMEA messages
        :param tuple block_size_range: (min, max) block size range
        """
        client_info = f"{self.client_address[0]}:{self.client_address[1]}"
        delay = getattr(self.server, "delay", 0.25)

        # Create queue for communication between producer and consumer threads
        message_queue = queue.Queue(maxsize=1000)  # Buffer up to 1000 messages
        producer_finished = threading.Event()
        consumer_stop = threading.Event()

        try:
            logger.info(f"Starting file mode for client {client_info}")
            logger.debug(f"Reading from file: {input_file}")

            # Start producer thread (reads from file)
            producer_thread = threading.Thread(
                target=self._producer_thread,
                args=(input_file, message_queue, producer_finished, consumer_stop),
                daemon=True,
            )
            producer_thread.start()

            # Start consumer thread (sends to socket)
            consumer_thread = threading.Thread(
                target=self._consumer_thread,
                args=(
                    message_queue,
                    block_size_range,
                    delay,
                    producer_finished,
                    consumer_stop,
                    client_info,
                ),
                daemon=True,
            )
            consumer_thread.start()

            # Wait for consumer thread to finish
            consumer_thread.join()

            logger.info(f"Session ended for {client_info}.")

        except (ConnectionAbortedError, BrokenPipeError):
            logger.info(f"Client disconnected: {client_info}")
        except Exception as e:
            logger.error(
                f"Error handling client {client_info}: {e}",
                exc_info=True,
            )
        finally:
            # Signal threads to stop
            consumer_stop.set()

    def _producer_thread(
        self,
        input_file: str,
        message_queue: queue.Queue,
        producer_finished: threading.Event,
        consumer_stop: threading.Event,
    ):
        """
        Producer thread that reads messages from file and puts them in queue.

        :param str input_file: Path to input file
        :param queue.Queue message_queue: Queue to put messages into
        :param threading.Event producer_finished: Event to signal when producer is done
        :param threading.Event consumer_stop: Event to check if consumer wants to stop
        """
        try:
            logger.debug("Producer thread started")

            while not consumer_stop.is_set():
                message_count = 0

                with open(input_file, "rb") as stream:
                    reader = NMEAReader(stream, nmeaonly=True, quitonerror=1)

                    for _, parsed_msg in reader:
                        if consumer_stop.is_set():
                            break

                        if parsed_msg is not None:
                            # Store original message without timestamp update
                            try:
                                # Put message in queue with timeout to allow checking consumer_stop
                                message_queue.put(parsed_msg, timeout=0.1)
                                message_count += 1
                            except queue.Full:
                                # Queue is full, check if consumer is still running
                                if consumer_stop.is_set():
                                    break
                                continue

                if message_count > 0:
                    logger.debug(f"Producer loaded {message_count} messages from file")

                # If we reach here, we've finished one pass through the file
                # Continue looping unless consumer has stopped
                if not consumer_stop.is_set():
                    logger.debug("Producer restarting file read (looping)")

        except Exception as e:
            logger.error(f"Producer thread error: {e}", exc_info=True)
        finally:
            producer_finished.set()
            logger.debug("Producer thread finished")

    def _consumer_thread(
        self,
        message_queue: queue.Queue,
        block_size_range: tuple,
        delay: float,
        producer_finished: threading.Event,
        consumer_stop: threading.Event,
        client_info: str,
    ):
        """
        Consumer thread that takes messages from queue and sends them to socket.

        :param queue.Queue message_queue: Queue to get messages from
        :param tuple block_size_range: (min, max) block size range
        :param float delay: Delay between message blocks
        :param threading.Event producer_finished: Event indicating producer is done
        :param threading.Event consumer_stop: Event to signal consumer to stop
        :param str client_info: Client information for logging
        """
        try:
            logger.debug("Consumer thread started")

            while not consumer_stop.is_set():
                block_size = random.randint(*block_size_range)
                data = bytearray()
                messages_in_block = 0

                # Collect messages for this block
                for _ in range(block_size):
                    try:
                        # Get message from queue with timeout
                        msg = message_queue.get(timeout=0.1)
                        # Update timestamp when consuming the message
                        updated_msg = self._update_message_timestamp(msg)
                        data += updated_msg.serialize()
                        messages_in_block += 1
                        message_queue.task_done()
                    except queue.Empty:
                        # No message available, check if producer is finished
                        if producer_finished.is_set() and message_queue.empty():
                            logger.debug(
                                "No more messages available and producer finished"
                            )
                            consumer_stop.set()
                            break
                        # Otherwise continue with whatever messages we have
                        break

                # Send the block if we have data
                if data and not self._is_client_disconnected():
                    try:
                        # Log raw data being sent in debug mode
                        logger.debug(
                            f"Raw data ({len(data)} bytes): {data[:100]}{'...' if len(data) > 100 else ''}"
                        )
                        self.wfile.write(data)
                        self.wfile.flush()
                        logger.debug(
                            f"Successfully sent block ({messages_in_block} messages) to {client_info}"
                        )
                    except (ConnectionAbortedError, BrokenPipeError):
                        logger.info(f"Client {client_info} disconnected during send")
                        consumer_stop.set()
                        break
                elif self._is_client_disconnected():
                    logger.info(f"Client {client_info} disconnected")
                    consumer_stop.set()
                    break

                # Delay between blocks
                sleep(delay)

        except Exception as e:
            logger.error(f"Consumer thread error: {e}", exc_info=True)
        finally:
            consumer_stop.set()
            logger.debug("Consumer thread finished")

    def _update_message_timestamp(self, msg: NMEAMessage) -> NMEAMessage:
        """
        Update NMEA message with current timestamp.

        :param NMEAMessage msg: Original NMEA message
        :return: Updated NMEA message with current timestamp
        :rtype: NMEAMessage
        """
        try:
            current_time = datetime.now(timezone.utc)

            # Create a new message with updated timestamp
            payload_dict = {}

            # Copy existing payload attributes
            for attr in dir(msg):
                if not attr.startswith("_") and hasattr(msg, attr):
                    value = getattr(msg, attr)
                    if not callable(value) and attr not in (
                        "msgID",
                        "talker",
                        "msgmode",
                        "payload",
                        "checksum",
                    ):
                        payload_dict[attr] = value
            # logger.debug(f"Payload: {payload_dict}")

            # Update time-related fields if they exist
            if hasattr(msg, "time"):
                payload_dict["time"] = current_time.time()
            if hasattr(msg, "date"):
                payload_dict["date"] = current_time.date()

            # Create new message with updated payload
            new_msg = NMEAMessage(msg.talker, msg.msgID, msg.msgmode, **payload_dict)
            # logger.debug(f"Updated message: {new_msg}")
            return new_msg

        except Exception as e:
            logger.warning(f"Error updating timestamp for message {msg.msgID}: {e}")
            return msg  # Return original message if update fails

    def _is_client_disconnected(self) -> bool:
        """
        Check if client is still connected.

        :return: True if client disconnected
        :rtype: bool
        """
        try:
            # Try to write empty data to check connection
            self.wfile.write(b"")
            self.wfile.flush()
            return False
        except (ConnectionAbortedError, BrokenPipeError):
            logger.debug(
                f"Client {self.client_address[0]}:{self.client_address[1]} connection check failed"
            )
            return True


def parse_arguments(args):
    """
    Parse command line arguments.

    :return: Parsed arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="TCP server that streams NMEA messages from file with updated timestamps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("input_file", help="Path to input NMEA file")

    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind to")

    parser.add_argument(
        "--port", type=int, default=50010, help="Port number to listen on"
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Delay between message blocks in seconds",
    )

    parser.add_argument(
        "--block-size-min", type=int, default=1, help="Minimum messages per block"
    )

    parser.add_argument(
        "--block-size-max", type=int, default=3, help="Maximum messages per block"
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    return parser.parse_args(args)


def run(args):
    """
    Run the TCP server with the given arguments.

    :param args: Arguments object with server configuration
    :type args: argparse.Namespace or object with same attributes
    """
    args = parse_arguments(args)

    # Configure logging
    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    # Validate input file
    if not Path(args.input_file).exists():
        logger.error(f"File '{args.input_file}' not found")
        exit(1)

    # Validate block size range
    if args.block_size_min > args.block_size_max:
        logger.error("Block size minimum cannot be greater than maximum")
        exit(1)

    if args.block_size_min < 1:
        logger.error("Block size minimum must be at least 1")
        exit(1)

    logger.info(f"Creating TCP server on {args.host}:{args.port}")
    logger.info(f"Reading NMEA messages from: {args.input_file}")
    logger.info(
        f"Block size range: {args.block_size_min}-{args.block_size_max} messages"
    )
    logger.info(f"Delay between blocks: {args.delay}s")

    server = ThreadingTCPServer((args.host, args.port), TestServer)

    # Store configuration in server instance
    server.input_file = args.input_file
    server.block_size_range = (args.block_size_min, args.block_size_max)
    server.delay = args.delay

    logger.info("Starting TCP server, waiting for client connections...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("TCP server terminated by user")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
    finally:
        logger.info("TCP server shutdown complete")


def main():
    """
    Main entry point.
    """
    run(sys.argv[1:])


if __name__ == "__main__":
    main()

    # ## FOR TESTING ONLY
    # args = [
    #     "gnssdata_filtered.bin",
    #     "--host",
    #     "localhost",
    #     "--port",
    #     "50016",
    #     "--delay",
    #     "0.25",
    #     "-vv",
    # ]
    # run(args)
