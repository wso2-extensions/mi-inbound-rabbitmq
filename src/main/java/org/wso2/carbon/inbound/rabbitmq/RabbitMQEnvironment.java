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


import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.BackOffDelayPolicy;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConnectionSettings;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.synapse.SynapseException;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;




/**
 * This class represents the RabbitMQ environment configuration and connection management.
 * It initializes and manages the AMQP environment, handles connection retries,
 * and provides SSL/TLS support for secure communication.
 */
public class RabbitMQEnvironment {


    private static final Log log = LogFactory.getLog(RabbitMQEnvironment.class);
    private final Map<String, String> rabbitMQProperties = new HashMap<>();
    private Environment environment = null;
    private Executor dispatchedExecutor;
    private final AtomicLong count = new AtomicLong(0);
    String inboundName = null;
    private int retryCount;
    private long retryInterval;
    private final RabbitMQRoundRobinAddressSelector addressSelector;
    private String logPrefix = null;


    /**
     * Constructs a `RabbitMQEnvironment` instance with the given inbound name, properties, and address selector.
     *
     * @param inboundName     The name of the inbound endpoint.
     * @param properties      The RabbitMQ configuration properties.
     * @param addressSelector The address selector for RabbitMQ connections.
     * @throws RabbitMQException If an error occurs during the initialization of the AMQP environment.
     */
    public RabbitMQEnvironment(String inboundName, Properties properties,
                               RabbitMQRoundRobinAddressSelector addressSelector) throws RabbitMQException {
        // Populate RabbitMQ properties from the provided properties object
        properties.stringPropertyNames().forEach(param ->
                rabbitMQProperties.put(param, properties.getProperty(param)));

        this.inboundName = inboundName;
        this.addressSelector = addressSelector;
        this.logPrefix = "Initializing RabbitMQ Inbound Endpoint: [" + inboundName + "]";

        // Initialize the AMQP environment
        initAmqpEnvironment();
    }

    /**
     * Initializes the AMQP environment with the provided RabbitMQ properties.
     * This includes setting up connection settings, authentication mechanisms,
     * and SSL/TLS configurations.
     *
     * @throws RabbitMQException If an error occurs during initialization.
     */
    private void initAmqpEnvironment() throws RabbitMQException {

        // Retrieve SASL mechanism, defaulting to the predefined value if not provided
        String saslMechanism = StringUtils.defaultIfEmpty(
                rabbitMQProperties.get(RabbitMQConstants.SASL_MECHANISM),
                RabbitMQConstants.DEFAULT_SASL_MECHANISM
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.SASL_MECHANISM)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.SASL_MECHANISM + " is not provided. Using default: "
                    + RabbitMQConstants.DEFAULT_SASL_MECHANISM);
        }

        // Retrieve virtual host, defaulting to the predefined value if not provided
        String virtualHost = StringUtils.defaultIfEmpty(
                rabbitMQProperties.get(RabbitMQConstants.SERVER_VIRTUAL_HOST),
                RabbitMQConstants.DEFAULT_VIRTUAL_HOST
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.SERVER_VIRTUAL_HOST)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.SERVER_VIRTUAL_HOST + " is not provided. Using default: "
                    + RabbitMQConstants.DEFAULT_VIRTUAL_HOST);
        }

        // Retrieve connection idle timeout, defaulting to the predefined value if not provided
        int connectionIdleTimeout = NumberUtils.toInt(
                rabbitMQProperties.get(RabbitMQConstants.IDLE_TIMEOUT),
                RabbitMQConstants.DEFAULT_IDLE_TIMEOUT
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.IDLE_TIMEOUT)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.IDLE_TIMEOUT + " is not provided. Using default: "
                    + RabbitMQConstants.DEFAULT_IDLE_TIMEOUT);
        }

        // Retrieve retry interval and retry count, defaulting to predefined values if not provided
        this.retryInterval = NumberUtils.toInt(
                rabbitMQProperties.get(RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_INTERVAL),
                RabbitMQConstants.DEFAULT_RETRY_INTERVAL
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_INTERVAL)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_INTERVAL
                    + " is not provided. Using default: " + RabbitMQConstants.DEFAULT_RETRY_INTERVAL);
        }

        this.retryCount = NumberUtils.toInt(
                rabbitMQProperties.get(RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_COUNT),
                RabbitMQConstants.DEFAULT_RETRY_COUNT
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_COUNT)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.CONNECTION_ESTABLISH_RETRY_COUNT
                    + " is not provided. Using default: " + RabbitMQConstants.DEFAULT_RETRY_COUNT);
        }

        // Check if SSL is enabled, defaulting to false if not provided
        boolean sslEnabled = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBoolean(rabbitMQProperties.get(RabbitMQConstants.SSL_ENABLED)), false);
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.SSL_ENABLED)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.SSL_ENABLED
                    + " is not provided. Using default: false");
        }

        // Initialize the AMQP environment builder and set connection settings
        AmqpEnvironmentBuilder environmentBuilder = new AmqpEnvironmentBuilder();
        environmentBuilder.dispatchingExecutor(getDispatchingExecutor());
        AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings =
                environmentBuilder.connectionSettings();
        connectionSettings.addressSelector(addressSelector);
        connectionSettings.uris(getAddressList(sslEnabled));
        connectionSettings.virtualHost(virtualHost);

        // Configure authentication based on the SASL mechanism
        if (saslMechanism.equalsIgnoreCase(ConnectionSettings.SASL_MECHANISM_PLAIN)) {
            configurePlainAuthentication(connectionSettings);
        } else if (saslMechanism.equalsIgnoreCase(ConnectionSettings.SASL_MECHANISM_EXTERNAL)) {
            configureExternalAuthentication(connectionSettings, sslEnabled);
        }

        // Set connection idle timeout
        connectionSettings.idleTimeout(Duration.ofMillis(connectionIdleTimeout));

        // Configure SSL/TLS settings if SSL is enabled
        if (sslEnabled) {
            configureSSL(connectionSettings);
        }

        // Build the AMQP environment
        this.environment = environmentBuilder.build();
    }

    /**
     * Configures plain authentication for the connection.
     *
     * @param connectionSettings The connection settings to configure.
     * @throws RabbitMQException If required properties are missing.
     */
    private void configurePlainAuthentication(AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings)
            throws RabbitMQException {
        connectionSettings.saslMechanism(ConnectionSettings.SASL_MECHANISM_PLAIN);

        boolean oauth2Enabled = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBoolean(
                        rabbitMQProperties.get(RabbitMQConstants.OAUTH2_ENABLED)),
                false
        );

        if (oauth2Enabled) {
            configureOAuth2Authentication(connectionSettings);
        } else {
            String username = StringUtils.defaultIfEmpty(
                    rabbitMQProperties.get(RabbitMQConstants.SERVER_USER_NAME),
                    RabbitMQConstants.DEFAULT_USER
            );
            if (!rabbitMQProperties.containsKey(RabbitMQConstants.SERVER_USER_NAME)) {
                log.warn(logPrefix + " - " + RabbitMQConstants.SERVER_USER_NAME + " is not provided. Using default: "
                        + RabbitMQConstants.DEFAULT_USER);
            }

            String password = StringUtils.defaultIfEmpty(
                    rabbitMQProperties.get(RabbitMQConstants.SERVER_PASSWORD),
                    RabbitMQConstants.DEFAULT_PASSWORD
            );
            if (!rabbitMQProperties.containsKey(RabbitMQConstants.SERVER_PASSWORD)) {
                log.warn(logPrefix + " - " + RabbitMQConstants.SERVER_PASSWORD + " is not provided. Using default: "
                        + RabbitMQConstants.DEFAULT_PASSWORD);
            }

            connectionSettings.username(username);
            connectionSettings.password(password);
        }
    }

    /**
     * Configures OAuth2 authentication for the connection.
     *
     * @param connectionSettings The connection settings to configure.
     * @throws RabbitMQException If required OAuth2 properties are missing.
     */
    private void configureOAuth2Authentication(AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings)
            throws RabbitMQException {
        String tokenEndpoint = rabbitMQProperties.get(RabbitMQConstants.OAUTH2_TOKEN_ENDPOINT);
        String clientId = rabbitMQProperties.get(RabbitMQConstants.OAUTH2_CLIENT_ID);
        String clientSecret = rabbitMQProperties.get(RabbitMQConstants.OAUTH2_CLIENT_SECRET);

        String[] missingOauth2Properties = {
                StringUtils.isEmpty(tokenEndpoint) ? RabbitMQConstants.OAUTH2_TOKEN_ENDPOINT : null,
                StringUtils.isEmpty(clientId) ? RabbitMQConstants.OAUTH2_CLIENT_ID : null,
                StringUtils.isEmpty(clientSecret) ? RabbitMQConstants.OAUTH2_CLIENT_SECRET : null,
        };

        Arrays.stream(missingOauth2Properties)
                .filter(StringUtils::isNotEmpty)
                .forEach(property -> log.info(logPrefix + " - " + property + " is not provided."));

        if (Arrays.stream(missingOauth2Properties).anyMatch(StringUtils::isNotEmpty)) {
            log.error(logPrefix + " - Required OAuth2 properties are not provided properly.");
            throw new SynapseException("Required OAuth2 properties are not provided properly.");
        }

        String grantType = StringUtils.defaultIfEmpty(
                rabbitMQProperties.get(RabbitMQConstants.OAUTH2_GRANT_TYPE),
                RabbitMQConstants.DEFAULT_OAUTH2_GRANT_TYPE
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.OAUTH2_GRANT_TYPE)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.OAUTH2_GRANT_TYPE + " is not provided. Using default: "
                    + RabbitMQConstants.DEFAULT_OAUTH2_GRANT_TYPE);
        }

        if (grantType.equalsIgnoreCase(RabbitMQConstants.OAUTH2_PASSWORD_GRANT_TYPE)) {
            configurePasswordGrant(connectionSettings, tokenEndpoint, clientId, clientSecret);
        } else {
            connectionSettings.oauth2()
                    .tokenEndpointUri(tokenEndpoint)
                    .clientId(clientId).clientSecret(clientSecret)
                    .grantType(grantType)
                    .shared(true);
        }

        connectionSettings.oauth2().tls().sslContext(getOAuth2SSLContext());
    }

    /**
     * Configures password grant type for OAuth2 authentication.
     *
     * @param connectionSettings The connection settings to configure.
     * @param tokenEndpoint      The token endpoint URI.
     * @param clientId           The client ID.
     * @param clientSecret       The client secret.
     * @throws RabbitMQException If required properties are missing.
     */
    private void configurePasswordGrant(
            AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings,
            String tokenEndpoint, String clientId, String clientSecret) throws RabbitMQException {
        String userName = rabbitMQProperties.get(RabbitMQConstants.OAUTH2_USERNAME);
        String password = rabbitMQProperties.get(RabbitMQConstants.OAUTH2_PASSWORD);

        String[] missingGrantTypeProperties = {
                StringUtils.isEmpty(userName) ? RabbitMQConstants.OAUTH2_USERNAME : null,
                StringUtils.isEmpty(password) ? RabbitMQConstants.OAUTH2_PASSWORD : null,
        };

        Arrays.stream(missingGrantTypeProperties)
                .filter(StringUtils::isNotEmpty)
                .forEach(property -> log.info(logPrefix + " - " + property + " is not provided."));

        if (Arrays.stream(missingGrantTypeProperties).anyMatch(StringUtils::isNotEmpty)) {
            log.error(logPrefix + " - Required Password Grant Type properties are not provided properly.");
            throw new SynapseException("Required Password Grant Type properties are not provided properly.");
        }

        connectionSettings.oauth2()
                .tokenEndpointUri(tokenEndpoint)
                .clientId(clientId).clientSecret(clientSecret)
                .grantType(RabbitMQConstants.OAUTH2_PASSWORD_GRANT_TYPE)
                .parameter(RabbitMQConstants.OAUTH2_USERNAME_PARAMETER, userName)
                .parameter(RabbitMQConstants.OAUTH2_PASSWORD_PARAMETER, password)
                .shared(true);
    }

    /**
     * Configures external authentication for the connection.
     *
     * @param connectionSettings The connection settings to configure.
     * @param sslEnabled         Whether SSL is enabled.
     * @throws RabbitMQException If SSL is not enabled for external authentication.
     */
    private void configureExternalAuthentication(
            AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings,
            boolean sslEnabled) throws RabbitMQException {
        connectionSettings.saslMechanism(ConnectionSettings.SASL_MECHANISM_EXTERNAL);
        if (!sslEnabled) {
            throw new RabbitMQException("SASL Mechanism EXTERNAL requires SSL to be enabled.");
        }
    }

    /**
     * Configures SSL/TLS settings for the connection.
     *
     * @param connectionSettings The connection settings to configure.
     */
    private void configureSSL(AmqpEnvironmentBuilder.EnvironmentConnectionSettings connectionSettings) {
        boolean trustEverything = Boolean.parseBoolean(rabbitMQProperties.get(RabbitMQConstants.SSL_TRUST_EVERYTHING));
        if (trustEverything) {
            log.warn(logPrefix + " - " + RabbitMQConstants.SSL_TRUST_EVERYTHING + " is enabled. " +
                    "This is insecure and should not be used in production environments.");
            connectionSettings.tls().trustEverything();
        } else {
            boolean hostnameVerification = Boolean.parseBoolean(
                    rabbitMQProperties.getOrDefault(RabbitMQConstants.SSL_VERIFY_HOSTNAME,
                            "true")
            );
            log.info(logPrefix + " - Hostname verification is " + (hostnameVerification ? "enabled" : "disabled")
                    + " for the AMQP connection.");
            connectionSettings.tls().sslContext(getSSLContext()).hostnameVerification(hostnameVerification);
        }
    }

    /**
     * Retrieves the dispatching executor for managing message receiver threads.
     * If the executor is not already initialized, it creates a fixed thread pool
     * with a user-defined or default thread pool size.
     *
     * @return The dispatching executor.
     */
    private Executor getDispatchingExecutor() {
        if (dispatchedExecutor == null) {
            // Get the maximum thread count from properties or use the default value
            int maxThreadCount = NumberUtils.toInt(
                    rabbitMQProperties.get(RabbitMQConstants.MESSAGE_RECEIVER_THREAD_POOL_SIZE),
                    RabbitMQConstants.DEFAULT_MESSAGE_RECEIVER_THREAD_POOL_SIZE);
            // Create a fixed thread pool with a custom thread factory
            dispatchedExecutor = Executors.newFixedThreadPool(maxThreadCount, createThreadFactory());
        }
        return dispatchedExecutor;
    }

    /**
     * Creates a custom thread factory for naming threads in the dispatching executor.
     *
     * @return A thread factory that names threads with the inbound name and a unique identifier.
     */
    private ThreadFactory createThreadFactory() {
        return r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setName(inboundName +
                    RabbitMQConstants.MESSAGE_RECEIVER_THREAD_NAME_PREFIX +
                    count.getAndIncrement());
            return thread;
        };
    }


    /**
     * Retrieves a list of RabbitMQ server addresses in the format `amqp://hostname:port`.
     * Validates that the number of hostnames matches the number of ports.
     *
     * @param sslEnabled Whether SSL is enabled, which determines the default port.
     * @return An array of RabbitMQ server addresses.
     * @throws SynapseException If the number of hostnames does not match the number of ports
     *                          or if a port number is invalid.
     */
    private String[] getAddressList(boolean sslEnabled) {
        // Retrieve hostnames, defaulting to the predefined value if not provided
        String[] hostnameArray = StringUtils.defaultIfEmpty(
                rabbitMQProperties.get(RabbitMQConstants.SERVER_HOST_NAME),
                RabbitMQConstants.DEFAULT_HOST
        ).split(",");
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.SERVER_HOST_NAME)) {
            log.warn(logPrefix + " - Hostname is not defined. Using default hostname: "
                    + RabbitMQConstants.DEFAULT_HOST);
        }

        // Retrieve ports, defaulting to the appropriate default based on SSL
        String[] portArray = StringUtils.defaultIfEmpty(
                rabbitMQProperties.get(RabbitMQConstants.SERVER_PORT),
                sslEnabled ? String.valueOf(RabbitMQConstants.DEFAULT_TLS_PORT)
                        : String.valueOf(RabbitMQConstants.DEFAULT_PORT)
        ).split(",");
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.SERVER_PORT)) {
            log.warn(logPrefix + " - Port is not defined. Using default port: " +
                    (sslEnabled ? RabbitMQConstants.DEFAULT_TLS_PORT : RabbitMQConstants.DEFAULT_PORT));
        }

        // Ensure the number of hostnames matches the number of ports
        if (hostnameArray.length != portArray.length) {
            throw new SynapseException("The number of hostnames must be equal to the number of ports");
        }

        // Construct the address list in the format `amqp://hostname:port`
        return IntStream.range(0, hostnameArray.length)
                .mapToObj(i -> {
                    try {
                        int port = Integer.parseInt(portArray[i].trim()); // Validate port number
                        return "amqp://" + hostnameArray[i].trim() + ":" + port;
                    } catch (NumberFormatException e) {
                        throw new SynapseException("Number format error in port number", e);
                    }
                })
                .toArray(String[]::new);
    }

    /**
     * Creates a connection to the RabbitMQ Broker. If the initial connection attempt fails,
     * it retries based on the configured retry policy.
     *
     * @return A RabbitMQ `Connection` instance.
     * @throws RabbitMQException If all connection attempts fail.
     */
    public Connection createConnection() throws RabbitMQException {
        try {
            return buildConnection();
        } catch (AmqpException.AmqpConnectionException e) {
            log.error("[" + inboundName + "] Error creating connection to RabbitMQ Broker. Retrying...", e);
            return retry();
        }
    }

    /**
     * Builds a new RabbitMQ connection using the configured environment.
     *
     * @return A RabbitMQ `Connection` instance.
     * @throws RabbitMQException If the connection could not be established.
     */
    private Connection buildConnection() throws RabbitMQException {
        Connection connection = environment.connectionBuilder()
                .recovery().backOffDelayPolicy(getRecoveryPolicy())
                .connectionBuilder()
                .listeners(new RabbitMQConnectionStateListener(inboundName))
                .build();

        if (connection == null) {
            throw new RabbitMQException("[" + inboundName + "] Could not connect to RabbitMQ Broker.");
        }
        log.info("[" + inboundName + "] Successfully connected to RabbitMQ Broker");
        return connection;
    }

    /**
     * Retries the connection to the RabbitMQ Broker based on the configured retry count and interval.
     *
     * @return A RabbitMQ `Connection` instance.
     * @throws RabbitMQException If all retry attempts are exhausted.
     */
    public Connection retry() throws RabbitMQException {
        int attempts = 0;

        // Retry until the maximum retry count is reached or indefinitely if retryCount is -1
        while (attempts < retryCount || retryCount == -1) {
            attempts++;
            log.info("[" + inboundName + "] Retrying connection to RabbitMQ Broker in " +
                    retryInterval + "ms. Attempt: " + attempts);

            try {
                Thread.sleep(retryInterval); // Wait before retrying
                return buildConnection();   // Attempt to build the connection
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore the interrupted status
                break;
            } catch (AmqpException.AmqpConnectionException e) {
                log.error("[" + inboundName + "] Retry attempt " + attempts + " failed.", e);
            }
        }

        throw new RabbitMQException("[" + inboundName + "] Could not connect to RabbitMQ Broker. " +
                "All retry attempts exhausted.");
    }

    /**
     * Retrieves the recovery policy for RabbitMQ connection retries.
     * The policy determines the delay and timeout behavior during connection recovery.
     *
     * @return A `BackOffDelayPolicy` instance based on the configured recovery policy.
     */
    private BackOffDelayPolicy getRecoveryPolicy() {
        // Retrieve the recovery policy type or use the default
        String policyType = rabbitMQProperties.getOrDefault(
                RabbitMQConstants.CONNECTION_RECOVERY_POLICY,
                String.valueOf(
                        RabbitMQConstants.ConnectionRecoveryPolicy.FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT
                )
        );
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.CONNECTION_RECOVERY_POLICY)) {
            log.warn(logPrefix + " - " + RabbitMQConstants.CONNECTION_RECOVERY_POLICY
                    + " is not provided. Using default: "
                    + RabbitMQConstants.ConnectionRecoveryPolicy.FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT);
        }

        // Retrieve initial delay, retry interval, and timeout values or use defaults
        Duration initialDelay = Duration.ofMillis(NumberUtils.toLong(
                rabbitMQProperties.get(RabbitMQConstants.CONNECTION_RECOVERY_INITIAL_DELAY),
                RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_INITIAL_DELAY
        ));
        Duration delay = Duration.ofMillis(NumberUtils.toLong(
                rabbitMQProperties.get(RabbitMQConstants.CONNECTION_RECOVERY_RETRY_INTERVAL),
                RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_RETRY_INTERVAL
        ));
        Duration timeout = Duration.ofMillis(NumberUtils.toLong(
                rabbitMQProperties.get(RabbitMQConstants.CONNECTION_RECOVERY_RETRY_TIMEOUT),
                RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_RETRY_TIMEOUT
        ));

        // Log warnings if properties are not provided
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.CONNECTION_RECOVERY_INITIAL_DELAY)) {
            if (log.isDebugEnabled()) {
                log.debug(logPrefix + " - " + RabbitMQConstants.CONNECTION_RECOVERY_INITIAL_DELAY
                        + " is not provided. Using default: "
                        + RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_INITIAL_DELAY + "ms");
            }
        }
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.CONNECTION_RECOVERY_RETRY_INTERVAL)) {
            if (log.isDebugEnabled()) {
                log.debug(logPrefix + " - " + RabbitMQConstants.CONNECTION_RECOVERY_RETRY_INTERVAL
                        + " is not provided. Using default: "
                        + RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_RETRY_INTERVAL + "ms");
            }
        }
        if (!rabbitMQProperties.containsKey(RabbitMQConstants.CONNECTION_RECOVERY_RETRY_TIMEOUT)) {
            if (log.isDebugEnabled()) {
                log.debug(logPrefix + " - " + RabbitMQConstants.CONNECTION_RECOVERY_RETRY_TIMEOUT
                        + " is not provided. Using default: "
                        + RabbitMQConstants.DEFAULT_CONNECTION_RECOVERY_RETRY_TIMEOUT + "ms");
            }
        }

        // Return the appropriate recovery policy based on the policy type
        switch (RabbitMQConstants.ConnectionRecoveryPolicy.valueOf(policyType)) {
            case FIXED:
                return BackOffDelayPolicy.fixed(delay);
            case FIXED_WITH_INITIAL_DELAY:
                return BackOffDelayPolicy.fixedWithInitialDelay(initialDelay, delay);
            case FIXED_WITH_INITIAL_DELAY_AND_TIMEOUT:
            default:
                return BackOffDelayPolicy
                        .fixedWithInitialDelay(initialDelay, delay, timeout);
        }
    }

    /**
     * Creates and initializes an SSLContext for OAuth2 authentication using the truststore details.
     * This method retrieves truststore properties from system properties,
     * validates them, and configures the SSLContext.
     * @return The initialized SSLContext instance for OAuth2 authentication.
     * @throws SynapseException If required truststore properties are missing or invalid.
     */
    private SSLContext getOAuth2SSLContext() {
        try {
            // Retrieve truststore properties from system properties
            String trustStore = System.getProperty(RabbitMQConstants.SERVER_TRUSTSTORE);
            String trustStorePassword = System.getProperty(RabbitMQConstants.SERVER_TRUSTSTORE_PASSWORD);
            String trustStoreType = StringUtils.defaultIfEmpty(
                    System.getProperty(RabbitMQConstants.SERVER_TRUSTSTORE_TYPE),
                    KeyStore.getDefaultType()
            );

            // Initialize the TrustManagerFactory using the truststore details
            TrustManagerFactory tmf = initTrustManagerFactory(trustStore, trustStoreType, trustStorePassword);

            // Create and initialize the SSLContext
            SSLContext sslContext = SSLContext.getInstance(RabbitMQConstants.SERVER_TLS);
            sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
            return sslContext;
        } catch (Exception e) {
            log.error("Error in initializing SSL context from Server configuration.", e);
            throw new SynapseException("Error in initializing SSL context from Server configuration", e);
        }
    }

    /**
     * Creates and initializes an SSLContext using the provided keystore and truststore details.
     * Validates the required SSL properties and ensures proper configuration.
     *
     * @return The initialized SSLContext instance.
     * @throws SynapseException If required SSL properties are missing or invalid.
     */
    private SSLContext getSSLContext() {
        SSLContext sslContext = null;
        try {
            // Retrieve SSL-related properties from the configuration
            String keyStoreLocation = rabbitMQProperties.get(RabbitMQConstants.SSL_KEYSTORE_LOCATION);
            String keyStoreType = StringUtils.defaultIfEmpty(
                    rabbitMQProperties.get(RabbitMQConstants.SSL_KEYSTORE_TYPE),
                    RabbitMQConstants.DEFAULT_KEYSTORE_TYPE
            );
            String keyStorePassword = rabbitMQProperties.get(RabbitMQConstants.SSL_KEYSTORE_PASSWORD);
            String trustStoreLocation = rabbitMQProperties.get(RabbitMQConstants.SSL_TRUSTSTORE_LOCATION);
            String trustStoreType = StringUtils.defaultIfEmpty(
                    rabbitMQProperties.get(RabbitMQConstants.SSL_TRUSTSTORE_TYPE),
                    RabbitMQConstants.DEFAULT_KEYSTORE_TYPE
            );
            String trustStorePassword = rabbitMQProperties.get(RabbitMQConstants.SSL_TRUSTSTORE_PASSWORD);
            String sslVersion = rabbitMQProperties.get(RabbitMQConstants.SSL_VERSION);

            // Validate that all required SSL properties are provided
            String[] missingProperties = {
                    StringUtils.isEmpty(keyStoreLocation) ? RabbitMQConstants.SSL_KEYSTORE_LOCATION : null,
                    StringUtils.isEmpty(keyStorePassword) ? RabbitMQConstants.SSL_KEYSTORE_PASSWORD : null,
                    StringUtils.isEmpty(trustStoreLocation) ? RabbitMQConstants.SSL_TRUSTSTORE_LOCATION : null,
                    StringUtils.isEmpty(trustStorePassword) ? RabbitMQConstants.SSL_TRUSTSTORE_PASSWORD : null
            };

            Arrays.stream(missingProperties)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(property -> log.info(logPrefix + " - " + property + " is not provided."));

            if (Arrays.stream(missingProperties).anyMatch(StringUtils::isNotEmpty)) {
                log.error(logPrefix + " - Required SSL properties are not provided properly.");
                throw new SynapseException("Required SSL properties are not provided properly.");
            }

            // Initialize KeyManagerFactory and TrustManagerFactory using helper methods
            KeyManagerFactory kmf = initKeyManagerFactory(keyStoreLocation, keyStoreType, keyStorePassword);
            TrustManagerFactory tmf = initTrustManagerFactory(trustStoreLocation, trustStoreType, trustStorePassword);

            // Determine the SSL version to use, defaulting to the predefined value if not provided
            String effectiveSslVersion = StringUtils.defaultIfEmpty(sslVersion, RabbitMQConstants.DEFAULT_SSL_VERSION);
            try {
                if (StringUtils.isEmpty(sslVersion)) {
                    log.warn(logPrefix + " - " + RabbitMQConstants.SSL_VERSION
                            + " is not specified. Hence using default SSL version: "
                            + effectiveSslVersion);
                }
                // Initialize the SSLContext with the KeyManager and TrustManager
                sslContext = SSLContext.getInstance(effectiveSslVersion);
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            } catch (NoSuchAlgorithmException e) {
                throw new SynapseException("Invalid SSL version: " + effectiveSslVersion
                        + ". Available SSL versions are: " + String.join(
                                ", ",
                        SSLContext.getDefault().getSupportedSSLParameters().getProtocols()),
                        e
                );
            }
        } catch (Exception e) {
            log.error("Format error in SSL enabled value.", e);
            throw new SynapseException("Format error in SSL enabled value", e);
        }
        return sslContext;
    }

    /**
     * Initializes a `KeyManagerFactory` using the provided keystore details.
     *
     * @param location The location of the keystore file.
     * @param type     The type of the keystore (e.g., JKS, PKCS12).
     * @param password The password for the keystore.
     * @return The initialized `KeyManagerFactory` instance.
     * @throws Exception If an error occurs during initialization.
     */
    private KeyManagerFactory initKeyManagerFactory(String location,
                                                    String type,
                                                    String password) throws Exception {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(Files.newInputStream(Paths.get(location)), password.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password.toCharArray());
        return kmf;
    }

    /**
     * Initializes a `TrustManagerFactory` using the provided truststore details.
     *
     * @param location The location of the truststore file.
     * @param type     The type of the truststore (e.g., JKS, PKCS12).
     * @param password The password for the truststore.
     * @return The initialized `TrustManagerFactory` instance.
     * @throws Exception If an error occurs during initialization.
     */
    private TrustManagerFactory initTrustManagerFactory(String location,
                                                        String type,
                                                        String password) throws Exception {
        KeyStore tks = KeyStore.getInstance(type);
        tks.load(Files.newInputStream(Paths.get(location)), password.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);
        return tmf;
    }

    /**
     * Closes the RabbitMQ environment and shuts down the executor service.
     * Ensures proper cleanup of resources.
     */
    public void close() {
        // Shut down the executor service if it is initialized
        if (dispatchedExecutor instanceof java.util.concurrent.ExecutorService) {
            ((java.util.concurrent.ExecutorService) dispatchedExecutor).shutdown();
            dispatchedExecutor = null;
        }
        // Close the AMQP environment if it is initialized
        if (environment != null) {
            environment.close();
            environment = null;
        }
    }
}
