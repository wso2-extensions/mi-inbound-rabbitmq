/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.inbound.rabbitmq;

import com.rabbitmq.client.amqp.Management;
/**
 * This class defines a collection of constants used for configuring and interacting
 * with RabbitMQ messaging services. It includes queue names, exchange names,
 * routing keys, and other RabbitMQ-related configuration values.
 * The constants in this class help ensure consistency and reduce the risk of
 * hardcoding values throughout the application.
 */
public class RabbitMQConstants {

    // --- Threading and Internal Constants ---
    // ---------------------------------------

    /**
     * Prefix for the names of the message receiver threads.
     */
    public static final String MESSAGE_RECEIVER_THREAD_NAME_PREFIX = "-rabbitmq-inbound-listener-dispatcher-";

    /**
     * Property key for the size of the message consumer thread pool.
     */
    public static final String MESSAGE_RECEIVER_THREAD_POOL_SIZE = "rabbitmq.message.consumer.thread.pool.size";
    public static final int DEFAULT_MESSAGE_RECEIVER_THREAD_POOL_SIZE = 10;

    /**
     * Internal constant for marking a transaction for rollback.
     */
    public static final String SET_ROLLBACK_ONLY = "SET_ROLLBACK_ONLY";
    /**
     * Internal constant to indicate requeuing on transaction rollback.
     */
    public static final String SET_REQUEUE_ON_ROLLBACK = "SET_REQUEUE_ON_ROLLBACK";
    /**
     * The delay (in milliseconds) before a message is requeued after failure.
     */
    public static final String MESSAGE_REQUEUE_DELAY = "rabbitmq.message.requeue.delay";
    /**
     * Property key for the maximum time (in milliseconds) to wait during shutdown.
     */
    public static final String MAX_WAIT_TIME_MILLIS = "rabbitmq.shutdown.max.wait.time.millis";
    public static final long DEFAULT_MAX_WAIT_TIME_MILLIS = 30000; // 30 seconds

    /**
     * Internal constant used for message acknowledgement decision.
     */
    public static final String ACKNOWLEDGEMENT_DECISION = "ACK_DECISION";

    // --- Connection Properties ---
    // -----------------------------

    public static final String SERVER_HOST_NAME = "rabbitmq.server.host.name";
    static final String DEFAULT_HOST = "localhost"; // Note: Package-private default for internal use

    public static final String SERVER_PORT = "rabbitmq.server.port";
    static final int DEFAULT_PORT = 5672; // Default non-TLS port
    public static final int DEFAULT_TLS_PORT = 5671; // Default TLS/SSL port

    public static final String SERVER_VIRTUAL_HOST = "rabbitmq.server.virtual.host";
    public static final String DEFAULT_VIRTUAL_HOST = "/";

    public static final String SERVER_USER_NAME = "rabbitmq.server.user.name";
    public static final String DEFAULT_USER = "guest";

    public static final String SERVER_PASSWORD = "rabbitmq.server.password";
    public static final String DEFAULT_PASSWORD = "guest";

    /**
     * Connection idle timeout (in milliseconds).
     */
    public static final String IDLE_TIMEOUT = "rabbitmq.connection.idle.timeout";
    public static final int DEFAULT_IDLE_TIMEOUT = 60000; // 60 seconds

    // --- Connection Establishment Retry ---
    // --------------------------------------

    /**
     * Interval (in milliseconds) between connection establishment retries.
     */
    public static final String CONNECTION_ESTABLISH_RETRY_INTERVAL = "rabbitmq.connection.establish.retry.interval";
    public static final int DEFAULT_RETRY_INTERVAL = 30000; // 30 seconds

    /**
     * Maximum number of connection establishment retries.
     */
    public static final String CONNECTION_ESTABLISH_RETRY_COUNT = "rabbitmq.connection.establish.retry.count";
    public static final int DEFAULT_RETRY_COUNT = 3;

    // --- Connection Recovery Properties ---
    // --------------------------------------

    /**
     * The policy used for connection recovery. Maps to {@link ConnectionRecoveryPolicy}.
     */
    public static final String CONNECTION_RECOVERY_POLICY = "rabbitmq.connection.recovery.policy.type";

    /**
     * Initial delay (in milliseconds) before the first recovery attempt.
     */
    public static final String CONNECTION_RECOVERY_INITIAL_DELAY = "rabbitmq.connection.recovery.initial.delay";
    public static final long DEFAULT_CONNECTION_RECOVERY_INITIAL_DELAY = 10000; // 10 seconds

    /**
     * Interval (in milliseconds) between recovery retries.
     */
    public static final String CONNECTION_RECOVERY_RETRY_INTERVAL = "rabbitmq.connection.recovery.retry.interval";
    public static final long DEFAULT_CONNECTION_RECOVERY_RETRY_INTERVAL = 10000; // 10 seconds

    /**
     * Maximum timeout (in milliseconds) for the entire recovery process.
     */
    public static final String CONNECTION_RECOVERY_RETRY_TIMEOUT = "rabbitmq.connection.recovery.retry.timeout";
    public static final long DEFAULT_CONNECTION_RECOVERY_RETRY_TIMEOUT = 60000; // 60 seconds

    // --- SSL/TLS Properties ---
    // --------------------------

    public static final String SSL_ENABLED = "rabbitmq.connection.ssl.enabled";
    public static final String SSL_TRUST_EVERYTHING = "rabbitmq.connection.ssl.trust.everything";
    public static final String SSL_KEYSTORE_LOCATION = "rabbitmq.connection.ssl.keystore.location";
    public static final String SSL_KEYSTORE_TYPE = "rabbitmq.connection.ssl.keystore.type";
    public static final String SSL_KEYSTORE_PASSWORD = "rabbitmq.connection.ssl.keystore.password";
    public static final String SSL_TRUSTSTORE_LOCATION = "rabbitmq.connection.ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_TYPE = "rabbitmq.connection.ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_PASSWORD = "rabbitmq.connection.ssl.truststore.password";
    public static final String SSL_VERSION = "rabbitmq.connection.ssl.version";
    public static final String DEFAULT_SSL_VERSION = "TLSv1.3";
    public static final String SSL_VERIFY_HOSTNAME = "rabbitmq.connection.ssl.hostname.verify.enabled";

    // System properties for default Java truststore configuration
    public static final String SERVER_TRUSTSTORE = "javax.net.ssl.trustStore";
    public static final String SERVER_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
    public static final String SERVER_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";
    public static final String SERVER_TLS = "TLS";

    public static final String DEFAULT_KEYSTORE_TYPE = "JKS";

    // --- SASL/OAuth2 Properties ---
    // ------------------------------

    public static final String SASL_MECHANISM = "rabbitmq.connection.sasl.mechanism";
    public static final String DEFAULT_SASL_MECHANISM = "PLAIN";

    public static final String OAUTH2_ENABLED = "rabbitmq.connection.oauth2.enabled";
    public static final String OAUTH2_TOKEN_ENDPOINT = "rabbitmq.connection.oauth2.token.endpoint";
    public static final String OAUTH2_GRANT_TYPE = "rabbitmq.connection.oauth2.grant.type";
    public static final String DEFAULT_OAUTH2_GRANT_TYPE = "client_credentials";
    public static final String OAUTH2_PASSWORD_GRANT_TYPE = "password";
    public static final String OAUTH2_USERNAME_PARAMETER = "username";
    public static final String OAUTH2_PASSWORD_PARAMETER = "password";
    public static final String OAUTH2_CLIENT_ID = "rabbitmq.connection.oauth2.client.id";
    public static final String OAUTH2_CLIENT_SECRET = "rabbitmq.connection.oauth2.client.secret";
    public static final String OAUTH2_USERNAME = "rabbitmq.connection.oauth2.username";
    public static final String OAUTH2_PASSWORD = "rabbitmq.connection.oauth2.password";

    // --- Exchange Properties ---
    // ---------------------------

    public static final String EXCHANGE_NAME = "rabbitmq.exchange.name";
    public static final String EXCHANGE_TYPE = "rabbitmq.exchange.type";
    public static final String DEFAULT_EXCHANGE_TYPE = "DIRECT"; // Using String value of Management.ExchangeType.DIRECT
    public static final String EXCHANGE_ARGUMENTS = "rabbitmq.exchange.arguments";
    public static final String EXCHANGE_AUTODECLARE = "rabbitmq.exchange.autodeclare";
    public static final String EXCHANGE_AUTO_DELETE = "rabbitmq.exchange.auto.delete";
    public static final String ROUTING_KEY = "rabbitmq.routing.key";
    /**
     * Arguments used for binding a queue to a header exchange.
     */
    public static final String HEADER_EXCHANGE_ARGUMENTS = "rabbitmq.header.exchange.binding.arguments";

    // --- Queue Properties ---
    // ------------------------

    public static final String QUEUE_NAME = "rabbitmq.queue.name";
    public static final String QUEUE_TYPE = "rabbitmq.queue.type";
    public static final String DEFAULT_QUEUE_TYPE = "CLASSIC";
    public static final String QUEUE_ARGUMENTS = "rabbitmq.queue.arguments";
    public static final String QUEUE_AUTODECLARE = "rabbitmq.queue.autodeclare";
    public static final String QUEUE_AUTO_DELETE = "rabbitmq.queue.auto.delete";
    public static final String QUEUE_EXCLUSIVE = "rabbitmq.queue.exclusive";

    /**
     * Initial credit value for consumers (especially for Stream consumers).
     */
    public static final String CONSUMER_INITIAL_CREDIT = "rabbitmq.consumer.initial.credit";
    public static final int DEFAULT_CONSUMER_INITIAL_CREDIT = 1;

    /**
     * Strategy for handling queue overflow. Maps to Management.OverflowStrategy.
     */
    public static final String QUEUE_OVERFLOW_STRATEGY = "rabbitmq.queue.overflow.strategy";
    // Assuming Management.OverflowStrategy is an Enum/Class available at compile time
    public static final Management.OverflowStrategy DEFAULT_QUEUE_OVERFLOW_STRATEGY =
            Management.OverflowStrategy.DROP_HEAD;

    // --- Message Properties and Headers ---
    // --------------------------------------

    public static final String CONTENT_TYPE = "rabbitmq.message.content.type";
    public static final String CONTENT_ENCODING = "rabbitmq.message.content.encoding";
    public static final String MESSAGE_ID = "rabbitmq.message.id";
    public static final String CORRELATION_ID = "rabbitmq.message.correlation.id";

    /**
     * Standard RabbitMQ header for the reply-to address in RPC patterns.
     */
    public static final String RABBITMQ_REPLY_TO = "RABBITMQ_REPLY_TO";
    /**
     * Standard RabbitMQ header to track the number of times a message has been dead-lettered.
     */
    public static final String X_OPT_DEATHS = "x-opt-deaths";
    /**
     * Internal constant to track if a message is part of an internal transaction.
     */
    public static final String INTERNAL_TRANSACTION_COUNTED = "INTERNAL_TRANSACTION_COUNTED";
    /**
     * Constant used to signal acknowledgment.
     */
    public static final String ACKNOWLEDGE = "ACKNOWLEDGE";
    /**
     * Header used by the Stream plugin to store the message offset.
     */
    public static final String X_STREAM_OFFSET = "x-stream-offset";

    // --- Acknowledgment Properties ---
    // ---------------------------------
    /**
        * Flag to enable automatic message acknowledgments.
     */
    public static final String AUTO_ACK_ENABLED = "rabbitmq.auto.ack.enabled";
    /**
     * Property for the maximum time (in milliseconds) to wait for publisher acknowledgments.
     */
    public static final String ACK_MAX_WAIT_TIME = "rabbitmq.ack.wait.time";
    public static final long DEFAULT_ACK_MAX_WAIT_TIME = 180000; // 3 minutes

    /**
     * Property key for the character set encoding.
     */
    public static final String CHARACTER_SET_ENCODING = "CHARACTER_SET_ENCODING";
    public static final String CHARSET = "charset";

    // --- Dead Letter Exchange/Queue (DLX/DLQ) Properties ---
    // -------------------------------------------------------

    public static final String DEAD_LETTER_QUEUE_NAME = "rabbitmq.dead.letter.queue.name";
    public static final String DEAD_LETTER_QUEUE_TYPE = "rabbitmq.dead.letter.queue.type";
    public static final String DEAD_LETTER_QUEUE_AUTO_DECLARE = "rabbitmq.dead.letter.queue.auto.declare";

    // 5 seconds TTL on DLQ messages for retry loop
    public static final long DEFAULT_DEAD_LETTER_QUEUE_MESSAGE_TTL = 5000;
    public static final String DEAD_LETTER_QUEUE_MESSAGE_TTL = "rabbitmq.dead.letter.queue.message.ttl";

    public static final String DEAD_LETTER_EXCHANGE_NAME = "rabbitmq.dead.letter.exchange.name";
    public static final String DEAD_LETTER_EXCHANGE_TYPE = "rabbitmq.dead.letter.exchange.type";
    public static final String DEAD_LETTER_EXCHANGE_AUTO_DECLARE = "rabbitmq.dead.letter.exchange.auto.declare";
    public static final String DEAD_LETTER_ROUTING_KEY = "rabbitmq.dead.letter.routing.key";

    /**
     * Max number of times a message will be dead-lettered before being routed to the final DLQ.
     */
    public static final String MAX_DEAD_LETTERED_COUNT = "rabbitmq.max.dead.lettered.count";

    // --- Final Dead Letter Exchange/Queue (Final DLX/DLQ) Properties ---
    // -------------------------------------------------------------------

    public static final String FINAL_DEAD_LETTER_QUEUE_NAME = "rabbitmq.final.dead.letter.queue.name";
    public static final String FINAL_DEAD_LETTER_QUEUE_TYPE = "rabbitmq.final.dead.letter.queue.type";
    public static final String FINAL_DEAD_LETTER_QUEUE_AUTO_DECLARE = "rabbitmq.final.dead.letter.queue.auto.declare";
    public static final String FINAL_DEAD_LETTER_EXCHANGE_NAME = "rabbitmq.final.dead.letter.exchange.name";
    public static final String FINAL_DEAD_LETTER_ROUTING_KEY = "rabbitmq.final.dead.letter.routing.key";
    public static final String FINAL_DEAD_LETTER_EXCHANGE_TYPE = "rabbitmq.final.dead.letter.exchange.type";
    public static final String FINAL_DEAD_LETTER_EXCHANGE_AUTO_DECLARE
            = "rabbitmq.final.dead.letter.exchange.auto.declare";

    // --- Dead Letter Publisher Retry Properties ---
    // ----------------------------------------------

    /**
     * Initial interval (in milliseconds) for retrying dead-letter publishing.
     */
    public static final String DEAD_LETTER_PUBLISHER_RETRY_INTERVAL = "rabbitmq.dead.letter.publisher.retry.interval";
    public static final long DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_INTERVAL = 10000; // 10 seconds

    /**
     * Maximum number of retries for dead-letter publishing.
     */
    public static final String DEAD_LETTER_PUBLISHER_RETRY_COUNT = "rabbitmq.dead.letter.publisher.retry.count";
    public static final int DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT = 3;

    /**
     * Factor for exponential backoff between retries.
     */
    public static final String DEAD_LETTER_PUBLISHER_RETRY_EXPONENTIAL_FACTOR
            = "rabbitmq.dead.letter.publisher.retry.exponential.factor";
    public static final float DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_EXPONENTIAL_FACTOR = 2.0F;

    /**
     * Timeout (in milliseconds) for the dead-letter publisher during shutdown.
     */
    public static final String DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT
            = "rabbitmq.dead.letter.publisher.shutdown.timeout";
    public static final long DEFAULT_DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT = 180000; // 3 minutes

    /**
     * Wait time (in milliseconds) for publisher acknowledgements in the dead-letter process.
     */
    public static final String DEAD_LETTER_PUBLISHER_ACK_WAIT_TIME = "rabbitmq.dead.letter.publisher.ack.wait.time";
    public static final long DEFAULT_DEAD_LETTER_PUBLISHER_ACK_WAIT_TIME = 30000; // 30 seconds

    /**
     * Flag to override classic queue requeue behavior with discard instead of requeueing.
     */
    public static final String OVERRIDE_CLASSIC_QUEUE_MESSAGE_REQUEUE_BEHAVIOR_WITH_DISCARD
            = "rabbitmq.classic.override.requeue.with.discard";

    // --- Classic Queue Specific Properties ---
    // -----------------------------------------

    public static final String CLASSIC_MAX_PRIORITY = "rabbitmq.classic.max.priority";
    public static final String CLASSIC_VERSION = "rabbitmq.classic.version";
    /**
     * The strategy to use when a message in a classic queue reaches its delivery limit.
     * Maps to {@link ClassicDeadLetterStrategy}.
     */
    public static final String CLASSIC_DEAD_LETTER_STRATEGY = "rabbitmq.classic.dead.letter.strategy";

    // --- Quorum Queue Specific Properties ---
    // ----------------------------------------

    public static final String QUORUM_INITIAL_MEMBER_COUNT = "rabbitmq.quorum.initial.member.count";
    public static final String QUORUM_DEAD_LETTER_STRATEGY = "rabbitmq.quorum.dead.letter.strategy";
    public static final String QUORUM_DELIVERY_LIMIT = "rabbitmq.quorum.delivery.limit";

    // --- Stream Specific Properties ---
    // ----------------------------------

    public static final String STREAM_FILTERS = "rabbitmq.stream.filters";
    public static final String STREAM_FILTER_MATCH_UNFILTERED = "rabbitmq.stream.filter.match.unfiltered";
    public static final String STREAM_OFFSET_TRACKER_FLUSH_INTERVAL = "rabbitmq.stream.offset.tracker.flush.interval";
    public static final long DEFAULT_STREAM_OFFSET_TRACKER_FLUSH_INTERVAL = 10;
    public static final String STREAM_OFFSET_TRACKER_SHUTDOWN_TIMEOUT
            = "rabbitmq.stream.offset.tracker.shutdown.timeout";
    public static final long DEFAULT_STREAM_OFFSET_TRACKER_SHUTDOWN_TIMEOUT = 5;

    /**
     * Annotation header used for stream filtering.
     */
    public static final String STREAM_FILTER_VALUE_ANNOTATION = "x-stream-filter-value";
    public static final String STREAM_INITIAL_MEMBER_COUNT = "rabbitmq.stream.initial.member.count";
    public static final String STREAM_MAX_AGE = "rabbitmq.stream.max.age";
    public static final String STREAM_MAX_SEGMENT_SIZE = "rabbitmq.stream.max.segment.size";
    public static final String STREAM_OFFSET_STARTING_VALUE = "rabbitmq.stream.offset.starting.value";
    public static final String STREAM_OFFSET_STARTING_STRATEGY = "rabbitmq.stream.offset.starting.strategy";

    // --- Registry/Persistence Properties ---
    // ---------------------------------------

    /**
     * The registry path for persisting connector state (e.g., stream offset).
     */
    public static final String REGISTRY_PATH = "inbound-connector/rabbitmq";
    /**
     * Resource name for the persisted offset.
     */
    public static final String RESOURCE_NAME = "offset";
    /**
     * Property name for the offset value within the resource.
     */
    public static final String PROPERTY_NAME = "offsetProperty";

    // --- Enums for Configuration ---
    // -------------------------------

    /**
     * Defines the available connection recovery policies.
     */
    public enum ConnectionRecoveryPolicy {
        FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT,
        FIXED_WITH_INITIAL_DELAY,
        FIXED;

        private ConnectionRecoveryPolicy() {
        }
    }

    /**
     * Defines the available dead-letter strategies for classic queues.
     */
    public enum ClassicDeadLetterStrategy {
        FIXED_DELAY_RETRYABLE_DISCARD,
        NON_RETRYABLE_DISCARD;

        private ClassicDeadLetterStrategy() {
        }
    }
}
