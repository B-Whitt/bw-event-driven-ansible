import asyncio
import io
import json
import logging
import os
from collections import OrderedDict
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED
from typing import Any
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context

# Optional Avro support
try:
    import fastavro

    HAS_FASTAVRO = True
except ImportError:
    HAS_FASTAVRO = False

# Optional Schema Registry support (official Confluent library)
try:
    from confluent_kafka.schema_registry import SchemaRegistryClient

    HAS_SCHEMA_REGISTRY = True
except ImportError:
    HAS_SCHEMA_REGISTRY = False

DOCUMENTATION = r"""
---
short_description: Receive events via a kafka topic.
description:
  - An ansible-rulebook event source plugin for receiving events via a kafka topic.
options:
  host:
    description:
      - The host where the kafka topic is hosted.
    type: str
    required: true
  port:
    description:
      - The port where the kafka server is listening.
    type: str
    required: true
  cafile:
    description:
      - The optional certificate authority file path containing certificates
        used to sign kafka broker certificates
    type: str
  certfile:
    description:
      - The optional client certificate file path containing the client
        certificate, as well as CA certificates needed to establish
        the certificate's authenticity.
    type: str
  keyfile:
    description:
      - The optional client key file path containing the client private key.
    type: str
  password:
    description:
      - The optional password to be used when loading the certificate chain.
    type: str
  check_hostname:
    description:
      - Enable SSL hostname verification.
    type: bool
    default: true
  verify_mode:
    description:
      - Whether to try to verify other peers' certificates and how to
        behave if verification fails.
    type: str
    default: "CERT_REQUIRED"
    choices: ["CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"]
  encoding:
    description:
      - Message encoding scheme.
    type: str
    default: "utf-8"
  topic:
    description:
      - The kafka topic. topic, topics, and topic_pattern are mutually exclusive.
    type: str
  topics:
    description:
      - The kafka topics. topic, topics, and topic_pattern are mutually exclusive.
    type: list
    elements: str
  topic_pattern:
    description:
      - The kafka topic pattern. It must be a valid regex. topic, topics, and topic_pattern are mutually exclusive.
        [AIOKafkaConsumer](https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer) performs
        periodic metadata refreshes in the background and will notice when new partitions are added to one of the
        subscribed topics or when a new topic matching a subscribed regex is created. See metadata_max_age_ms for
        more details on how to configure the metadata refresh.
    type: str
  metadata_max_age_ms:
    description:
      - The period of time in milliseconds for forcing a refresh of metadata.
        It configures how soon a topic or partition change is detected. Default to 5 minutes.
    type: int
    default: 300000 # 5 minutes
  group_id:
    description:
      - A kafka group id.
    type: str
    default: null
  offset:
    description:
      - Where to automatically reset the offset.
    type: str
    default: "latest"
    choices: ["latest", "earliest"]
  security_protocol:
    description:
      - Protocol used to communicate with brokers.
    type: str
    default: "PLAINTEXT"
    choices: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
  sasl_mechanism:
    description:
      - Authentication mechanism when security_protocol is configured.
    type: str
    default: "PLAIN"
    choices: ["PLAIN", "GSSAPI", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"]
  sasl_plain_username:
    description:
      - Username for SASL PLAIN authentication.
    type: str
  sasl_plain_password:
    description:
      - Password for SASL PLAIN authentication.
    type: str
  sasl_kerberos_service_name:
    description:
      - The service name, default is kafka
    type: str
  sasl_kerberos_domain_name:
    description:
      - The kerberos REALM
    type: str
  message_format:
    description:
      - The message serialization format.
      - When set to 'json', messages are decoded as JSON (with fallback to raw string).
      - When set to 'avro', messages are deserialized using Apache Avro.
      - Avro format requires the 'fastavro' package to be installed.
    type: str
    default: "json"
    choices: ["json", "avro"]
  schema_registry_url:
    description:
      - URL of the Confluent Schema Registry for Avro schema lookup.
      - Required when using 'avro' format with Schema Registry.
      - Requires the 'confluent-kafka[schemaregistry]' package.
      - Takes precedence over avro_schema_file if both are provided.
      - SSL settings (cafile, certfile, keyfile) are automatically applied
        to Schema Registry connections when configured.
    type: str
  schema_registry_basic_auth:
    description:
      - Basic authentication credentials for Schema Registry.
      - Format is 'username:password'.
    type: str
  schema_registry_bearer_token:
    description:
      - Static bearer token for Schema Registry authentication.
      - Use this when you have a pre-obtained token.
      - For automatic token management, use the OAuth client credentials options instead.
      - Mutually exclusive with schema_registry_basic_auth and OAuth options.
    type: str
  schema_registry_oauth_client_id:
    description:
      - OAuth client ID for Schema Registry authentication using client credentials grant.
      - Requires confluent-kafka version 2.9.0 or later.
      - Must be used together with schema_registry_oauth_client_secret and
        schema_registry_oauth_token_url.
    type: str
  schema_registry_oauth_client_secret:
    description:
      - OAuth client secret for Schema Registry authentication.
      - Used together with schema_registry_oauth_client_id.
    type: str
  schema_registry_oauth_token_url:
    description:
      - OAuth token endpoint URL for obtaining access tokens.
      - Example for Confluent Cloud is 'https://login.confluent.io/oauth/token'.
    type: str
  schema_registry_oauth_scope:
    description:
      - OAuth scope for the access token request.
      - Common values include 'schema:read' or space-separated list of scopes.
    type: str
  schema_registry_logical_cluster:
    description:
      - Confluent Cloud logical cluster ID for Schema Registry.
      - Format is 'lsrc-xxxxx'.
      - Required for Confluent Cloud OAuth authentication.
    type: str
  schema_registry_identity_pool_id:
    description:
      - Confluent Cloud identity pool ID for principal authorization.
      - Format is 'pool-xxxxx'.
      - Required for Confluent Cloud OAuth authentication.
    type: str
  avro_schema_file:
    description:
      - Path to a local Avro schema file (.avsc) for message deserialization.
      - Used when Schema Registry is not available.
      - The schema file should contain valid Avro schema JSON.
    type: str
  schema_registry_ssl:
    description:
      - Whether to apply Kafka SSL settings to Schema Registry connections.
      - When true, the cafile, certfile, and keyfile options are used for
        Schema Registry SSL/TLS connections.
      - Set to true when both Kafka and Schema Registry use the same
        certificates (common in enterprise deployments).
      - Set to false when Schema Registry is on an internal network without
        SSL, or when it uses different certificates than Kafka.
      - Only applies when schema_registry_url is configured.
    type: bool
    default: true
"""

EXAMPLES = r"""
# JSON format (default)
- ansible.eda.kafka:
    host: "localhost"
    port: "9092"
    check_hostname: true
    verify_mode: "CERT_OPTIONAL"
    encoding: "utf-8"
    topics:
      - "demo"
      - "demo2"
    group_id: "test"
    offset: "earliest"
    security_protocol: "SASL_PLAINTEXT"
    sasl_mechanism: "GSSAPI"
    sasl_plain_username: "admin"
    sasl_plain_password: "test"

# Avro format with Schema Registry
- ansible.eda.kafka:
    host: "localhost"
    port: "9092"
    topic: "avro-events"
    group_id: "eda-avro-consumer"
    message_format: "avro"
    schema_registry_url: "http://schema-registry:8081"

# Avro format with Schema Registry (authenticated)
- ansible.eda.kafka:
    host: "localhost"
    port: "9092"
    topic: "secure-avro-events"
    group_id: "eda-avro-consumer"
    message_format: "avro"
    schema_registry_url: "https://schema-registry:8081"
    schema_registry_basic_auth: "username:password"

# Avro format with local schema file
- ansible.eda.kafka:
    host: "localhost"
    port: "9092"
    topic: "avro-events"
    group_id: "eda-avro-consumer"
    message_format: "avro"
    avro_schema_file: "/path/to/schema.avsc"

# Avro format with static Bearer Token
- ansible.eda.kafka:
    host: "localhost"
    port: "9092"
    topic: "avro-events"
    group_id: "eda-avro-consumer"
    message_format: "avro"
    schema_registry_url: "https://schema-registry:8081"
    schema_registry_bearer_token: "{{ pre_obtained_token }}"

# Avro format with Confluent Cloud (OAuth Client Credentials Grant)
- ansible.eda.kafka:
    host: "pkc-xxxxx.us-east-1.aws.confluent.cloud"
    port: "9092"
    topic: "avro-events"
    group_id: "eda-avro-consumer"
    security_protocol: "SASL_SSL"
    sasl_mechanism: "OAUTHBEARER"
    sasl_plain_username: "{{ kafka_api_key }}"
    sasl_plain_password: "{{ kafka_api_secret }}"
    message_format: "avro"
    schema_registry_url: "https://psrc-xxxxx.us-east-1.aws.confluent.cloud"
    schema_registry_oauth_client_id: "{{ oauth_client_id }}"
    schema_registry_oauth_client_secret: "{{ oauth_client_secret }}"
    schema_registry_oauth_token_url: "https://login.confluent.io/oauth/token"
    schema_registry_oauth_scope: "schema:read"
    schema_registry_logical_cluster: "lsrc-xxxxx"
    schema_registry_identity_pool_id: "pool-xxxxx"
"""


class AvroDeserializer:
    """Handles Avro message deserialization with multiple schema sources."""

    # Confluent Schema Registry wire format: magic byte + 4-byte schema ID
    MAGIC_BYTE = 0
    SCHEMA_ID_SIZE = 4

    # Maximum message size to deserialize (1MB default, matches Kafka default)
    # Prevents memory exhaustion from oversized messages
    MAX_MESSAGE_SIZE = 1_048_576  # 1MB

    def __init__(
        self,
        schema_registry_url: str | None = None,
        schema_registry_basic_auth: str | None = None,
        schema_registry_bearer_token: str | None = None,
        schema_registry_oauth_client_id: str | None = None,
        schema_registry_oauth_client_secret: str | None = None,
        schema_registry_oauth_token_url: str | None = None,
        schema_registry_oauth_scope: str | None = None,
        schema_registry_logical_cluster: str | None = None,
        schema_registry_identity_pool_id: str | None = None,
        avro_schema_file: str | None = None,
        schema_registry_ssl: bool = True,
        ssl_cafile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
    ) -> None:
        """Initialize the Avro deserializer.

        Args:
            schema_registry_url: URL of the Confluent Schema Registry.
            schema_registry_basic_auth: Basic auth credentials (username:password).
            schema_registry_bearer_token: Static bearer token for authentication.
            schema_registry_oauth_client_id: OAuth client ID for client credentials grant.
            schema_registry_oauth_client_secret: OAuth client secret.
            schema_registry_oauth_token_url: OAuth token endpoint URL.
            schema_registry_oauth_scope: OAuth scope for access token.
            schema_registry_logical_cluster: Confluent Cloud logical cluster ID.
            schema_registry_identity_pool_id: Confluent Cloud identity pool ID.
            avro_schema_file: Path to a local Avro schema file.
            schema_registry_ssl: Whether to apply Kafka SSL settings to Schema Registry.
            ssl_cafile: Path to CA certificate file for SSL verification.
            ssl_certfile: Path to client certificate file for mTLS.
            ssl_keyfile: Path to client private key file for mTLS.
        """
        self.logger = logging.getLogger()
        self.schema_registry_client: Any = None
        self.local_schema: Any = None

        # Bounded LRU schema cache (max 100 entries) to prevent memory exhaustion
        # from malicious messages with many unique schema IDs
        self._schema_cache_max_size = 100
        self._schema_cache: OrderedDict[int, Any] = OrderedDict()

        # Determine which auth method is being used
        has_basic_auth = schema_registry_basic_auth is not None
        has_bearer_token = schema_registry_bearer_token is not None
        has_oauth = schema_registry_oauth_client_id is not None

        # Validate mutually exclusive auth options
        auth_methods = sum([has_basic_auth, has_bearer_token, has_oauth])
        if auth_methods > 1:
            msg = (
                "Only one authentication method can be used: "
                "schema_registry_basic_auth, schema_registry_bearer_token, "
                "or OAuth client credentials (schema_registry_oauth_client_id). "
                "Please provide only one."
            )
            raise ValueError(msg)

        # Validate OAuth client credentials - all required fields must be present
        if has_oauth:
            if not schema_registry_oauth_client_secret:
                msg = (
                    "schema_registry_oauth_client_secret is required "
                    "when using OAuth client credentials."
                )
                raise ValueError(msg)
            if not schema_registry_oauth_token_url:
                msg = (
                    "schema_registry_oauth_token_url is required "
                    "when using OAuth client credentials."
                )
                raise ValueError(msg)

        # Initialize Schema Registry client if URL provided
        if schema_registry_url:
            if not HAS_SCHEMA_REGISTRY:
                msg = (
                    "confluent-kafka[schemaregistry] package is required "
                    "for Schema Registry support. "
                    "Install it with: pip install confluent-kafka[schemaregistry]"
                )
                raise ImportError(msg)

            # Validate Schema Registry URL
            parsed_url = urlparse(schema_registry_url)
            if parsed_url.scheme not in ("http", "https"):
                msg = (
                    f"Invalid schema_registry_url scheme: '{parsed_url.scheme}'. "
                    "Only 'http' and 'https' are supported."
                )
                raise ValueError(msg)
            if not parsed_url.netloc:
                msg = (
                    f"Invalid schema_registry_url: '{schema_registry_url}'. "
                    "URL must include a host."
                )
                raise ValueError(msg)

            # Build config for official Confluent Schema Registry client
            sr_config: dict[str, Any] = {"url": schema_registry_url}

            if has_basic_auth:
                # Basic authentication
                sr_config["basic.auth.user.info"] = schema_registry_basic_auth
            elif has_bearer_token:
                # Static bearer token
                sr_config["bearer.auth.token"] = schema_registry_bearer_token
            elif has_oauth:
                # OAuth 2.0 Client Credentials Grant
                sr_config["bearer.auth.credentials.source"] = "OAUTHBEARER"
                sr_config["bearer.auth.issuer.endpoint.url"] = (
                    schema_registry_oauth_token_url
                )
                sr_config["bearer.auth.client.id"] = schema_registry_oauth_client_id
                sr_config["bearer.auth.client.secret"] = (
                    schema_registry_oauth_client_secret
                )
                if schema_registry_oauth_scope:
                    sr_config["bearer.auth.scope"] = schema_registry_oauth_scope
                if schema_registry_logical_cluster:
                    sr_config["bearer.auth.logical.cluster"] = (
                        schema_registry_logical_cluster
                    )
                if schema_registry_identity_pool_id:
                    sr_config["bearer.auth.identity.pool.id"] = (
                        schema_registry_identity_pool_id
                    )

            # Apply SSL configuration (reuses Kafka SSL settings when enabled)
            if schema_registry_ssl:
                if ssl_cafile:
                    sr_config["ssl.ca.location"] = ssl_cafile
                if ssl_certfile:
                    sr_config["ssl.certificate.location"] = ssl_certfile
                if ssl_keyfile:
                    sr_config["ssl.key.location"] = ssl_keyfile

            self.schema_registry_client = SchemaRegistryClient(sr_config)

        # Load local schema file if provided
        if avro_schema_file:
            if not HAS_FASTAVRO:
                msg = (
                    "fastavro package is required for Avro support. "
                    "Install it with: pip install fastavro"
                )
                raise ImportError(msg)

            # Validate schema file path
            schema_path = os.path.realpath(avro_schema_file)
            if not os.path.isfile(schema_path):
                msg = f"Avro schema file not found: {avro_schema_file}"
                raise FileNotFoundError(msg)

            with open(schema_path, encoding="utf-8") as f:
                schema_dict = json.load(f)
                self.local_schema = fastavro.parse_schema(schema_dict)

    def _get_schema_from_registry(self, schema_id: int) -> Any:
        """Fetch and cache schema from Schema Registry.

        Uses a bounded LRU cache to prevent memory exhaustion from
        malicious messages with many unique schema IDs.

        Args:
            schema_id: The schema ID from the message header.

        Returns:
            Parsed Avro schema.
        """
        # Check cache first
        if schema_id in self._schema_cache:
            self._schema_cache.move_to_end(schema_id)
            return self._schema_cache[schema_id]

        # Fetch from Schema Registry
        registered_schema = self.schema_registry_client.get_schema(schema_id)
        schema_dict = json.loads(registered_schema.schema_str)
        parsed_schema = fastavro.parse_schema(schema_dict)

        # Evict oldest entry if cache is full
        if len(self._schema_cache) >= self._schema_cache_max_size:
            self._schema_cache.popitem(last=False)

        # Add to cache (at end, as most recently used)
        self._schema_cache[schema_id] = parsed_schema

        return parsed_schema

    def _is_schema_registry_format(self, data: bytes) -> bool:
        """Check if message uses Confluent Schema Registry wire format.

        Args:
            data: Raw message bytes.

        Returns:
            True if message has Schema Registry format header.
        """
        return (
            len(data) > 5
            and data[0] == self.MAGIC_BYTE
        )

    def _extract_schema_id(self, data: bytes) -> int:
        """Extract schema ID from Confluent wire format message.

        Args:
            data: Raw message bytes.

        Returns:
            Schema ID as integer.
        """
        return int.from_bytes(data[1:5], byteorder="big")

    def deserialize(self, data: bytes) -> dict[str, Any] | None:
        """Deserialize Avro message using available schema source.

        Priority:
        1. Schema Registry (if message has wire format header)
        2. Local schema file
        3. Embedded schema (Object Container format)

        Args:
            data: Raw message bytes.

        Returns:
            Deserialized message as dictionary, or None on failure.
        """
        # Check message size to prevent memory exhaustion
        if len(data) > self.MAX_MESSAGE_SIZE:
            self.logger.warning(
                "Avro message size (%d bytes) exceeds maximum allowed (%d bytes). "
                "Message skipped to prevent memory exhaustion.",
                len(data),
                self.MAX_MESSAGE_SIZE,
            )
            return None

        # Try Schema Registry format first
        if self.schema_registry_client and self._is_schema_registry_format(data):
            schema_id = self._extract_schema_id(data)
            try:
                schema = self._get_schema_from_registry(schema_id)
                payload = data[5:]  # Skip magic byte + schema ID
                reader = io.BytesIO(payload)
                return fastavro.schemaless_reader(reader, schema)
            except Exception:
                self.logger.exception(
                    "Failed to deserialize with Schema Registry (schema_id=%s)",
                    schema_id,
                )
                return None

        # Try local schema
        if self.local_schema:
            try:
                reader = io.BytesIO(data)
                return fastavro.schemaless_reader(reader, self.local_schema)
            except Exception:
                self.logger.exception("Failed to deserialize with local schema")
                return None

        # Try embedded schema (Object Container format)
        # Note: Object Container format is typically used for Avro data files,
        # not Kafka messages. Kafka producers usually use Schema Registry
        # (Confluent wire format) or a pre-shared schema. If you're reaching
        # this fallback, consider configuring schema_registry_url or
        # avro_schema_file for better performance and reliability.
        self.logger.warning(
            "No Schema Registry or local schema configured. "
            "Attempting to deserialize as Avro Object Container format. "
            "This format is uncommon for Kafka messages. Consider configuring "
            "schema_registry_url or avro_schema_file for production use."
        )
        try:
            reader = io.BytesIO(data)
            avro_reader = fastavro.reader(reader)
            # Return first record (single message expected)
            for record in avro_reader:
                return record
            return None
        except Exception:
            self.logger.exception(
                "Failed to deserialize Avro message. No schema source succeeded. "
                "Ensure schema_registry_url or avro_schema_file is configured, "
                "or that the message uses Avro Object Container format."
            )
            return None


async def main(  # pylint: disable=R0914
    queue: asyncio.Queue[Any],
    args: dict[str, Any],
) -> None:
    """Receive events via a kafka topic."""
    logger = logging.getLogger()

    topic = args.get("topic")
    topics = args.get("topics")
    topic_pattern = args.get("topic_pattern")

    num_topics = sum(1 for tp in (topic, topics, topic_pattern) if tp is not None)
    if num_topics != 1:
        msg = "Exactly one of topic, topics, or topic_pattern must be provided."
        raise ValueError(msg)

    if topic:
        topics = [topic]

    host = args.get("host")
    port = args.get("port")
    cafile = args.get("cafile")
    certfile = args.get("certfile")
    keyfile = args.get("keyfile")
    password = args.get("password")
    check_hostname = args.get("check_hostname", True)
    verify_mode = args.get("verify_mode", "CERT_REQUIRED")
    group_id = args.get("group_id")
    offset = args.get("offset", "latest")
    encoding = args.get("encoding", "utf-8")
    security_protocol = args.get("security_protocol", "PLAINTEXT")

    # Avro configuration
    message_format = args.get("message_format", "json")
    schema_registry_url = args.get("schema_registry_url")
    schema_registry_basic_auth = args.get("schema_registry_basic_auth")
    schema_registry_bearer_token = args.get("schema_registry_bearer_token")
    schema_registry_ssl = args.get("schema_registry_ssl", True)
    avro_schema_file = args.get("avro_schema_file")

    # OAuth client credentials configuration
    schema_registry_oauth_client_id = args.get("schema_registry_oauth_client_id")
    schema_registry_oauth_client_secret = args.get("schema_registry_oauth_client_secret")
    schema_registry_oauth_token_url = args.get("schema_registry_oauth_token_url")
    schema_registry_oauth_scope = args.get("schema_registry_oauth_scope")
    schema_registry_logical_cluster = args.get("schema_registry_logical_cluster")
    schema_registry_identity_pool_id = args.get("schema_registry_identity_pool_id")

    # Validate message format
    if message_format not in ("json", "avro"):
        msg = f"Invalid message_format option: {message_format}"
        raise ValueError(msg)

    # Initialize Avro deserializer if needed
    avro_deserializer: AvroDeserializer | None = None
    if message_format == "avro":
        if not HAS_FASTAVRO:
            msg = (
                "fastavro package is required for Avro message format. "
                "Install it with: pip install fastavro"
            )
            raise ImportError(msg)

        avro_deserializer = AvroDeserializer(
            schema_registry_url=schema_registry_url,
            schema_registry_basic_auth=schema_registry_basic_auth,
            schema_registry_bearer_token=schema_registry_bearer_token,
            schema_registry_oauth_client_id=schema_registry_oauth_client_id,
            schema_registry_oauth_client_secret=schema_registry_oauth_client_secret,
            schema_registry_oauth_token_url=schema_registry_oauth_token_url,
            schema_registry_oauth_scope=schema_registry_oauth_scope,
            schema_registry_logical_cluster=schema_registry_logical_cluster,
            schema_registry_identity_pool_id=schema_registry_identity_pool_id,
            avro_schema_file=avro_schema_file,
            schema_registry_ssl=schema_registry_ssl,
            ssl_cafile=cafile,
            ssl_certfile=certfile,
            ssl_keyfile=keyfile,
        )

    if offset not in ("latest", "earliest"):
        msg = f"Invalid offset option: {offset}"
        raise ValueError(msg)

    verify_modes = {
        "CERT_NONE": CERT_NONE,
        "CERT_OPTIONAL": CERT_OPTIONAL,
        "CERT_REQUIRED": CERT_REQUIRED,
    }
    try:
        verify_mode = verify_modes[verify_mode]
    except KeyError as exc:
        msg = f"Invalid verify_mode option: {verify_mode}"
        raise ValueError(msg) from exc

    ssl_context = None
    if cafile or security_protocol.endswith("SSL"):
        security_protocol = security_protocol.replace("PLAINTEXT", "SSL")
        ssl_context = create_ssl_context(
            cafile=cafile,
            certfile=certfile,
            keyfile=keyfile,
            password=password,
        )
        ssl_context.check_hostname = check_hostname
        ssl_context.verify_mode = verify_mode

    kafka_consumer = AIOKafkaConsumer(
        bootstrap_servers=f"{host}:{port}",
        group_id=group_id,
        enable_auto_commit=True,
        max_poll_records=1,
        auto_offset_reset=offset,
        security_protocol=security_protocol,
        ssl_context=ssl_context,
        sasl_mechanism=args.get("sasl_mechanism", "PLAIN"),
        sasl_plain_username=args.get("sasl_plain_username"),
        sasl_plain_password=args.get("sasl_plain_password"),
        sasl_kerberos_service_name=args.get("sasl_kerberos_service_name"),
        sasl_kerberos_domain_name=args.get("sasl_kerberos_domain_name"),
        metadata_max_age_ms=int(args.get("metadata_max_age_ms", 300000)),
    )

    kafka_consumer.subscribe(topics=topics, pattern=topic_pattern)

    await kafka_consumer.start()

    try:
        await receive_msg(queue, kafka_consumer, encoding, avro_deserializer)
    finally:
        logger.info("Stopping kafka consumer")
        await kafka_consumer.stop()


async def receive_msg(
    queue: asyncio.Queue[Any],
    kafka_consumer: AIOKafkaConsumer,
    encoding: str,
    avro_deserializer: AvroDeserializer | None = None,
) -> None:
    """Receive messages from the Kafka topic and put them into the queue."""
    logger = logging.getLogger()

    async for msg in kafka_consumer:
        event: dict[str, Any] = {}

        # Process headers
        try:
            headers: dict[str, str] = {
                header[0]: header[1].decode(encoding) for header in msg.headers
            }
            event["meta"] = {}
            event["meta"]["headers"] = headers
        except UnicodeError:
            logger.exception("Unicode error while decoding headers")

        # Process message body
        data: dict[str, Any] | str | None = None

        if avro_deserializer:
            # Avro deserialization
            try:
                data = avro_deserializer.deserialize(msg.value)
            except Exception:
                logger.exception("Error deserializing Avro message")
                data = None
        else:
            # JSON deserialization (original behavior)
            try:
                value = msg.value.decode(encoding)
                data = json.loads(value)
            except json.decoder.JSONDecodeError:
                logger.info("JSON decode error, storing raw value")
                data = value
            except UnicodeError:
                logger.exception("Unicode error while decoding message body")
                data = None

        # Add data to the event and put it into the queue
        if data:
            event["body"] = data
            await queue.put(event)

        await asyncio.sleep(0)


if __name__ == "__main__":
    """MockQueue if running directly."""

    class MockQueue(asyncio.Queue[Any]):
        """A fake queue."""

        async def put(self: "MockQueue", event: dict[str, Any]) -> None:
            """Print the event."""
            print(event)  # noqa: T201

    asyncio.run(
        main(
            MockQueue(),
            {"topic": "eda", "host": "localhost", "port": "9092", "group_id": "test"},
        ),
    )
