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

import com.rabbitmq.client.amqp.ByteCapacity;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Management;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.rabbitmq.message.handler.AbstractRabbitMQMessageHandler;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import java.io.ByteArrayInputStream;
import java.time.Duration;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

/**
 * Utility class for RabbitMQ operations, providing methods for message handling,
 * queue and exchange management, and other RabbitMQ-related functionalities.
 */
public class RabbitMQUtils {
    private static final Log log = LogFactory.getLog(RabbitMQUtils.class);


    /**
     * Builds the message context by setting the correlation ID, content type, content encoding,
     * transaction properties, and SOAP envelope based on the RabbitMQ message context.
     *
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     * @param msgContext             The Axis2 message context to be populated.
     * @param serviceProperties      The service properties for additional configurations.
     * @throws AxisFault If an error occurs while setting the SOAP envelope.
     */
    public static void buildMessage(RabbitMQMessageContext rabbitMQMessageContext, MessageContext msgContext, Properties serviceProperties) throws AxisFault {
        setCorrelationId(msgContext, rabbitMQMessageContext); // Set the correlation ID for the message.
        String contentType = setContentType(msgContext, rabbitMQMessageContext, serviceProperties); // Determine and set the content type.
        setContentEncoding(msgContext, rabbitMQMessageContext, serviceProperties); // Set the content encoding if available.
        setTransactionCountedProperty(msgContext, rabbitMQMessageContext); // Set transaction-related properties.
        setSoapEnvelop(msgContext, rabbitMQMessageContext.getBody(), contentType); // Build and set the SOAP envelope.
    }

    /**
     * Sets the correlation ID in the message context. The correlation ID is determined
     * based on the RabbitMQ message context or the Axis2 message context.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     */
    private static void setCorrelationId(MessageContext msgContext, RabbitMQMessageContext rabbitMQMessageContext) {
        String correlationID;
        if (StringUtils.isNotEmpty(rabbitMQMessageContext.getCorrelationId())) {
            correlationID = rabbitMQMessageContext.getCorrelationId(); // Use the correlation ID from RabbitMQ context if available.
        } else if (StringUtils.isNotEmpty(rabbitMQMessageContext.getMessageID())) {
            correlationID = rabbitMQMessageContext.getMessageID(); // Use the message ID if correlation ID is not available.
        } else {
            correlationID = msgContext.getMessageID(); // Fallback to the Axis2 message ID.
        }

        msgContext.setProperty(RabbitMQConstants.CORRELATION_ID, correlationID); // Set the correlation ID in the message context.
    }

    /**
     * Determines and sets the content type in the message context. The content type is
     * derived from the RabbitMQ message context or service properties.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     * @param serviceProperties      The service properties for additional configurations.
     * @return The determined content type.
     */
    private static String setContentType(MessageContext msgContext, RabbitMQMessageContext rabbitMQMessageContext, Properties serviceProperties) {
        String contentTypeFromService = serviceProperties.getProperty(RabbitMQConstants.CONTENT_TYPE);
        String contentTypeFromMessage = rabbitMQMessageContext.getContentType();
        String contentType;

        if (StringUtils.isNotEmpty(contentTypeFromMessage)) {
            contentType = contentTypeFromMessage; // Use the content type from the RabbitMQ message context.
        } else if (StringUtils.isNotEmpty(contentTypeFromService)) {
            contentType = contentTypeFromService; // Use the content type from the service properties.
        } else {
            log.warn("Unable to determine content type for message " + msgContext.getMessageID() + ". Setting to text/plain.");
            contentType = "text/plain"; // Default to "text/plain" if no content type is available.
        }

        msgContext.setProperty(RabbitMQConstants.CONTENT_TYPE, contentType); // Set the content type in the message context.
        return contentType;
    }

    /**
     * Sets the content encoding in the message context. The content encoding is
     * derived from the RabbitMQ message context or service properties.
     *
     * @param msgContext             The Axis2 message context to be updated.
     * @param rabbitMQMessageContext The RabbitMQ message context containing message details.
     * @param serviceProperties      The service properties for additional configurations.
     */
    private static void setContentEncoding(MessageContext msgContext, RabbitMQMessageContext rabbitMQMessageContext, Properties serviceProperties) {
        String encodingFromService = serviceProperties.getProperty(RabbitMQConstants.CONTENT_ENCODING);
        String contentEncoding = StringUtils.isEmpty(encodingFromService) ? rabbitMQMessageContext.getContentEncoding() : encodingFromService;

        if (contentEncoding != null) {
            msgContext.setProperty(RabbitMQConstants.CONTENT_ENCODING, contentEncoding); // Set the content encoding in the message context.
        }
    }


    private static void setTransactionCountedProperty(MessageContext msgContext, RabbitMQMessageContext rabbitMQMessageContext) {
        if (rabbitMQMessageContext.hasProperties()) {
            Object transactionCounted = rabbitMQMessageContext.getApplicationProperties().get(RabbitMQConstants.INTERNAL_TRANSACTION_COUNTED);
            if (transactionCounted != null) {
                msgContext.setProperty(RabbitMQConstants.INTERNAL_TRANSACTION_COUNTED, transactionCounted);
            }
        }

    }

    /**
     * Sets the SOAP envelope in the message context based on the provided message body
     * and content type.
     *
     * @param msgContext  The Axis2 message context to be updated.
     * @param body        The message body as a byte array.
     * @param contentType The content type of the message.
     * @throws AxisFault If an error occurs while processing the SOAP envelope.
     */
    private static void setSoapEnvelop(MessageContext msgContext, byte[] body, String contentType) throws AxisFault {
        Builder builder = getBuilder(msgContext, contentType); // Get the appropriate builder for the content type.
        OMElement documentElement = builder.processDocument(new ByteArrayInputStream(body), contentType, msgContext); // Process the message body.
        msgContext.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement)); // Set the SOAP envelope in the message context.
    }

    /**
     * Retrieves the appropriate message builder for the given content type.
     * If no builder is found, defaults to the SOAP builder.
     *
     * @param msgContext     The Axis2 message context.
     * @param rawContentType The raw content type of the message.
     * @return The message builder for the content type.
     * @throws AxisFault If an error occurs while parsing the content type.
     */
    private static Builder getBuilder(MessageContext msgContext, String rawContentType) throws AxisFault {
        try {
            ContentType contentType = new ContentType(rawContentType); // Parse the content type.
            String charset = contentType.getParameter(RabbitMQConstants.CHARSET);
            msgContext.setProperty(RabbitMQConstants.CHARACTER_SET_ENCODING, charset); // Set the character set encoding.

            Builder builder = BuilderUtil.getBuilderFromSelector(contentType.getBaseType(), msgContext); // Get the builder for the base type.
            if (builder == null) {
                if (log.isDebugEnabled()) {
                    log.debug("No message builder found for type '" + contentType.getBaseType() + "'. Falling back to SOAP.");
                }
                builder = new SOAPBuilder(); // Default to the SOAP builder.
            }

            return builder;
        } catch (ParseException e) {
            throw new AxisFault("Error parsing content type: " + rawContentType, e); // Handle content type parsing errors.
        }
    }

    /**
     * Extracts transport headers from the RabbitMQ message context and Axis2 message context.
     * The method combines headers from both contexts, prioritizing RabbitMQ headers.
     *
     * @param rabbitMQMessageContext The RabbitMQ message context containing message-specific headers.
     * @param msgContext             The Axis2 message context containing additional headers.
     * @return A map of transport headers.
     */
    public static Map<String, String> getTransportHeaders(RabbitMQMessageContext rabbitMQMessageContext, MessageContext msgContext) {
        Map<String, String> map = new HashMap<>();

        // Add the correlation ID from the RabbitMQ message context if available.
        if (rabbitMQMessageContext.getCorrelationId() != null) {
            map.put(RabbitMQConstants.CORRELATION_ID, rabbitMQMessageContext.getCorrelationId());
        }

        // Add the message ID, prioritizing the RabbitMQ message context over the Axis2 message context.
        String messageId = rabbitMQMessageContext.getMessageID();
        if (messageId != null) {
            map.put(RabbitMQConstants.MESSAGE_ID, messageId);
        } else if (msgContext.getMessageID() != null) {
            map.put(RabbitMQConstants.MESSAGE_ID, msgContext.getMessageID());
        }

        // Add the reply-to header from the RabbitMQ message context if available.
        if (rabbitMQMessageContext.getReplyTo() != null) {
            map.put(RabbitMQConstants.RABBITMQ_REPLY_TO, rabbitMQMessageContext.getReplyTo());
        }

        // Add application properties from the RabbitMQ message context, excluding internal transaction properties.
        if (rabbitMQMessageContext.hasProperties()) {
            for (Map.Entry<String, Object> headerEntry : rabbitMQMessageContext.getApplicationProperties().entrySet()) {
                String key = headerEntry.getKey();
                Object value = headerEntry.getValue();
                if (key != null && value != null && !RabbitMQConstants.INTERNAL_TRANSACTION_COUNTED.equals(key)) {
                    map.put(key, value.toString());
                }
            }
        }

        return map;
    }

    /**
     * Creates a stream consumer for RabbitMQ. This consumer is configured to handle messages
     * from a stream queue with optional filters and a subscription listener.
     *
     * @param connection            The RabbitMQ connection to use.
     * @param messageHandler        The message handler to process incoming messages.
     * @param rabbitmqProperties    The RabbitMQ properties for configuration.
     * @param registryOffsetTracker The offset tracker for managing stream offsets.
     * @return A configured RabbitMQ stream consumer.
     */
    public static Consumer createStreamConsumer(Connection connection, AbstractRabbitMQMessageHandler messageHandler, Properties rabbitmqProperties, RabbitMQRegistryOffsetTracker registryOffsetTracker) {
        ConsumerBuilder consumerBuilder = connection.consumerBuilder();
        String queueName = rabbitmqProperties.getProperty(RabbitMQConstants.QUEUE_NAME);
        consumerBuilder.messageHandler(messageHandler).queue(queueName);

        // Configure stream filters if defined in the properties
        String streamFilters = rabbitmqProperties.getProperty(RabbitMQConstants.STREAM_FILTERS);
        if (StringUtils.isNotEmpty(streamFilters)) {
            String[] filterArray = streamFilters.split(",");
            boolean matchUnfiltered = BooleanUtils.toBoolean(rabbitmqProperties.getProperty(RabbitMQConstants.STREAM_FILTER_MATCH_UNFILTERED));
            consumerBuilder.stream().filterValues(filterArray).filterMatchUnfiltered(matchUnfiltered);
        } else {
            log.info("No stream filters defined. All messages in the stream queue will be consumed.");
        }

        // Add a subscription listener for the stream consumer
        consumerBuilder.subscriptionListener(new RabbitMQStreamSubscriptionListener(registryOffsetTracker, rabbitmqProperties));

        // Configure initial credits for the consumer
        configureInitialCredits(consumerBuilder, rabbitmqProperties);
        return consumerBuilder.build();
    }

    /**
     * Creates a default consumer for RabbitMQ. This consumer is configured for CLASSIC/QUORUM queue types.
     *
     * @param connection         The RabbitMQ connection to use.
     * @param messageHandler     The message handler to process incoming messages.
     * @param rabbitmqProperties The RabbitMQ properties for configuration.
     * @return A configured RabbitMQ default consumer.
     */
    public static Consumer createDefaultConsumer(Connection connection, AbstractRabbitMQMessageHandler messageHandler, Properties rabbitmqProperties) {
        ConsumerBuilder consumerBuilder = connection.consumerBuilder();
        String queueName = rabbitmqProperties.getProperty(RabbitMQConstants.QUEUE_NAME);
        consumerBuilder.messageHandler(messageHandler).queue(queueName);

        log.info("Configuring consumer for CLASSIC/QUORUM queue type.");

        // Configure initial credits for the consumer
        configureInitialCredits(consumerBuilder, rabbitmqProperties);
        return consumerBuilder.build();
    }

    /**
     * Configures the initial credits for a RabbitMQ consumer. Initial credits determine
     * the number of messages the consumer can process concurrently.
     *
     * @param consumerBuilder    The consumer builder to configure.
     * @param rabbitmqProperties The RabbitMQ properties for configuration.
     */
    private static void configureInitialCredits(ConsumerBuilder consumerBuilder, Properties rabbitmqProperties) {
        int initialCredits = NumberUtils.toInt(
                rabbitmqProperties.getProperty(RabbitMQConstants.CONSUMER_INITIAL_CREDIT),
                RabbitMQConstants.DEFAULT_CONSUMER_INITIAL_CREDIT);

        // Log a message if the initial credit property is not defined
        if (!rabbitmqProperties.containsKey(RabbitMQConstants.CONSUMER_INITIAL_CREDIT)) {
            log.info(RabbitMQConstants.CONSUMER_INITIAL_CREDIT + " is not defined. Using default: " + RabbitMQConstants.DEFAULT_CONSUMER_INITIAL_CREDIT);
        }

        // Set the initial credits for the consumer
        consumerBuilder.initialCredits(initialCredits);
    }

    /**
     * Declares a RabbitMQ queue based on the provided properties.
     * This method supports CLASSIC, QUORUM, and STREAM queue types and configures
     * various queue parameters such as auto-delete, exclusive access, overflow strategy,
     * and dead-letter queue settings.
     *
     * @param management The RabbitMQ management interface for queue operations.
     * @param queueName  The name of the queue to be declared.
     * @param properties The properties containing queue configuration details.
     */
    public static void declareQueue(Management management, String queueName, Properties properties) {
        // Validate if the queue name is provided and if auto-declare is enabled
        if (StringUtils.isEmpty(queueName) || !BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(properties.getProperty(RabbitMQConstants.QUEUE_AUTODECLARE)), true)) {
            return;
        }

        // Determine the queue type (CLASSIC, QUORUM, or STREAM)
        Management.QueueType queueType = Management.QueueType.valueOf(
                properties.getProperty(RabbitMQConstants.QUEUE_TYPE, RabbitMQConstants.DEFAULT_QUEUE_TYPE));
        Management.QueueSpecification queueBuilder = management.queue()
                .name(queueName)
                .type(queueType)
                .autoDelete(isAutoDeleteQueue(properties)); // Set auto-delete property

        // Configure the queue based on its type
        switch (queueType) {
            case CLASSIC:
                queueBuilder.exclusive(isExclusiveQueue(properties)); // Set exclusive property for CLASSIC queues
                configureClassicQueue(queueBuilder, properties); // Additional CLASSIC queue configurations
                break;
            case QUORUM:
                configureQuorumQueue(queueBuilder, properties); // Additional QUORUM queue configurations
                break;
            case STREAM:
                configureStreamQueue(queueBuilder, properties); // Additional STREAM queue configurations
                break;
        }

        // Configure optional queue arguments
        configureQueueArguments(queueBuilder, properties);

        // Configure overflow strategy and dead-letter queue for non-STREAM queues
        if (queueType != Management.QueueType.STREAM) {
            Management.OverflowStrategy overflowStrategy = properties.containsKey(RabbitMQConstants.QUEUE_OVERFLOW_STRATEGY)
                    ? Management.OverflowStrategy.valueOf(properties.getProperty(RabbitMQConstants.QUEUE_OVERFLOW_STRATEGY))
                    : RabbitMQConstants.DEFAULT_QUEUE_OVERFLOW_STRATEGY;

            // Adjust overflow strategy for QUORUM queues with incompatible dead-letter strategies
            if (queueType == Management.QueueType.QUORUM &&
                    RabbitMQConstants.QUORUM_DEAD_LETTER_STRATEGY.equalsIgnoreCase(
                            properties.getProperty(RabbitMQConstants.QUORUM_DEAD_LETTER_STRATEGY)) &&
                    overflowStrategy == Management.OverflowStrategy.DROP_HEAD) {
                overflowStrategy = Management.OverflowStrategy.REJECT_PUBLISH;
                log.warn("DROP_HEAD overflow strategy is not compatible with AT_LEAST_ONCE dead letter strategy for Quorum queues. Changing to REJECT_PUBLISH.");
            }

            queueBuilder.overflowStrategy(overflowStrategy); // Set the overflow strategy
            configureDeadLetterQueue(management, queueBuilder, queueType, properties); // Configure dead-letter queue
        }

        // Declare the queue
        queueBuilder.declare();
    }

    /**
     * Configures CLASSIC queue-specific properties such as max priority and version.
     *
     * @param queueBuilder The queue specification builder.
     * @param properties   The properties containing CLASSIC queue configurations.
     */
    private static void configureClassicQueue(Management.QueueSpecification queueBuilder, Properties properties) {
        if (properties.containsKey(RabbitMQConstants.CLASSIC_MAX_PRIORITY)) {
            int maxPriority = NumberUtils.toInt(properties.getProperty(RabbitMQConstants.CLASSIC_MAX_PRIORITY));
            queueBuilder.classic().maxPriority(maxPriority); // Set max priority for CLASSIC queues
        }
        if (properties.containsKey(RabbitMQConstants.CLASSIC_VERSION)) {
            Management.ClassicQueueVersion version = Management.ClassicQueueVersion.valueOf(properties.getProperty(RabbitMQConstants.CLASSIC_VERSION));
            queueBuilder.classic().version(version); // Set CLASSIC queue version
        }
    }

    /**
     * Configures QUORUM queue-specific properties such as initial member count,
     * delivery limit, and dead-letter strategy.
     *
     * @param queueBuilder The queue specification builder.
     * @param properties   The properties containing QUORUM queue configurations.
     */
    private static void configureQuorumQueue(Management.QueueSpecification queueBuilder, Properties properties) {
        if (properties.containsKey(RabbitMQConstants.QUORUM_INITIAL_MEMBER_COUNT)) {
            int initialMemberCount = NumberUtils.toInt(properties.getProperty(RabbitMQConstants.QUORUM_INITIAL_MEMBER_COUNT));
            queueBuilder.quorum().initialMemberCount(initialMemberCount); // Set initial member count for QUORUM queues
        }
        if (properties.containsKey(RabbitMQConstants.QUORUM_DELIVERY_LIMIT)) {
            int deliveryLimit = NumberUtils.toInt(properties.getProperty(RabbitMQConstants.QUORUM_DELIVERY_LIMIT));
            queueBuilder.quorum().deliveryLimit(deliveryLimit); // Set delivery limit for QUORUM queues
        }
        if (properties.containsKey(RabbitMQConstants.QUORUM_DEAD_LETTER_STRATEGY)) {
            Management.QuorumQueueDeadLetterStrategy deadLetterStrategy = Management.QuorumQueueDeadLetterStrategy.valueOf(properties.getProperty(RabbitMQConstants.QUORUM_DEAD_LETTER_STRATEGY));
            queueBuilder.quorum().deadLetterStrategy(deadLetterStrategy); // Set dead-letter strategy for QUORUM queues
        }
    }

    /**
     * Configures STREAM queue-specific properties such as initial member count,
     * maximum age, and maximum segment size.
     *
     * @param queueBuilder The queue specification builder.
     * @param properties   The properties containing STREAM queue configurations.
     */
    private static void configureStreamQueue(Management.QueueSpecification queueBuilder, Properties properties) {
        if (properties.containsKey(RabbitMQConstants.STREAM_INITIAL_MEMBER_COUNT)) {
            int initialMemberCount = NumberUtils.toInt(properties.getProperty(RabbitMQConstants.STREAM_INITIAL_MEMBER_COUNT));
            queueBuilder.stream().initialMemberCount(initialMemberCount); // Set initial member count for STREAM queues
        }
        if (properties.containsKey(RabbitMQConstants.STREAM_MAX_AGE)) {
            long maxAge = NumberUtils.toLong(properties.getProperty(RabbitMQConstants.STREAM_MAX_AGE));
            queueBuilder.stream().maxAge(Duration.ofSeconds(maxAge)); // Set maximum age for STREAM queues
        }
        if (properties.containsKey(RabbitMQConstants.STREAM_MAX_SEGMENT_SIZE)) {
            long maxSegmentSizeBytes = NumberUtils.toLong(properties.getProperty(RabbitMQConstants.STREAM_MAX_SEGMENT_SIZE));
            queueBuilder.stream().maxSegmentSizeBytes(ByteCapacity.B(maxSegmentSizeBytes)); // Set maximum segment size for STREAM queues
        }
    }

    /**
     * Configures optional queue arguments based on the provided properties.
     *
     * @param queueBuilder The queue specification builder.
     * @param properties   The properties containing optional queue arguments.
     */
    private static void configureQueueArguments(Management.QueueSpecification queueBuilder, Properties properties) {
        String arguments = properties.getProperty(RabbitMQConstants.QUEUE_ARGUMENTS);
        if (StringUtils.isNotEmpty(arguments)) {
            Map<String, Object> argumentMap = getOptionalArguments(properties, arguments);
            if (argumentMap != null) {
                argumentMap.forEach(queueBuilder::argument); // Add optional arguments to the queue
            }
        }
    }

    /**
     * Configures the dead-letter queue for the specified queue type.
     *
     * @param management   The RabbitMQ management interface for queue operations.
     * @param queueBuilder The queue specification builder.
     * @param queueType    The type of the queue (CLASSIC, QUORUM, or STREAM).
     * @param properties   The properties containing dead-letter queue configurations.
     */
    private static void configureDeadLetterQueue(Management management, Management.QueueSpecification queueBuilder, Management.QueueType queueType, Properties properties) {
        String deadLetterQueueName = null;
        String deadLetterExchange = null;
        String deadLetterStrategy = null;

        // Check if dead-letter queue auto-declaration is enabled
        if (BooleanUtils.toBooleanDefaultIfNull(BooleanUtils.toBooleanObject(properties.getProperty(RabbitMQConstants.DEAD_LETTER_QUEUE_AUTO_DECLARE)), false)) {
            deadLetterQueueName = properties.getProperty(RabbitMQConstants.DEAD_LETTER_QUEUE_NAME);
            Management.QueueType deadLetterQueueType = Management.QueueType.valueOf(properties.getProperty(RabbitMQConstants.DEAD_LETTER_QUEUE_TYPE, RabbitMQConstants.DEFAULT_QUEUE_TYPE));

            // Handle CLASSIC queue-specific dead-letter strategy
            if (queueType == Management.QueueType.CLASSIC) {
                deadLetterStrategy = properties.getProperty(RabbitMQConstants.CLASSIC_DEAD_LETTER_STRATEGY, RabbitMQConstants.ClassicDeadLetterStrategy.NON_RETRYABLE_DISCARD.name());

                if (RabbitMQConstants.ClassicDeadLetterStrategy.FIXED_DELAY_RETRYABLE_DISCARD.name().equalsIgnoreCase(deadLetterStrategy)) {
                    long messageTTL = NumberUtils.toLong(properties.getProperty(RabbitMQConstants.DEAD_LETTER_QUEUE_MESSAGE_TTL), RabbitMQConstants.DEFAULT_DEAD_LETTER_QUEUE_MESSAGE_TTL);
                    deadLetterQueueDeclare(management, deadLetterQueueName, deadLetterQueueType, messageTTL, properties.getProperty(RabbitMQConstants.EXCHANGE_NAME), properties.getProperty(RabbitMQConstants.ROUTING_KEY));
                } else {
                    deadLetterQueueDeclare(management, deadLetterQueueName, deadLetterQueueType, null, null, null);
                }
            } else {
                // Default dead-letter queue declaration for non-CLASSIC queues
                deadLetterQueueDeclare(management, deadLetterQueueName, deadLetterQueueType, null, null, null);
            }
        }

        // Configure dead-letter exchange if specified
        if (properties.containsKey(RabbitMQConstants.DEAD_LETTER_EXCHANGE_NAME)) {
            deadLetterExchange = properties.getProperty(RabbitMQConstants.DEAD_LETTER_EXCHANGE_NAME);
            Management.ExchangeType deadLetterExchangeType = Management.ExchangeType.valueOf(properties.getProperty(RabbitMQConstants.DEAD_LETTER_EXCHANGE_TYPE, RabbitMQConstants.DEFAULT_EXCHANGE_TYPE));

            if (BooleanUtils.toBooleanDefaultIfNull(BooleanUtils.toBooleanObject(properties.getProperty(RabbitMQConstants.DEAD_LETTER_EXCHANGE_AUTO_DECLARE)), false)) {
                deadLetterExchangeDeclare(management, deadLetterExchange, deadLetterExchangeType);
            }

            // Bind dead-letter exchange to queue if the exchange type is direct
            if (deadLetterQueueName != null && deadLetterExchangeType == Management.ExchangeType.DIRECT) {
                // use the exchange name if the routing key is not provided
                String deadLetterRoutingKey = properties.getProperty(RabbitMQConstants.DEAD_LETTER_ROUTING_KEY, deadLetterExchange);
                bindDeadLetterExchangeToQueue(management, deadLetterQueueName, deadLetterExchange, deadLetterRoutingKey);
                queueBuilder.deadLetterRoutingKey(deadLetterRoutingKey);
            }

            queueBuilder.deadLetterExchange(deadLetterExchange);
        }

        // Configure final dead-letter queue for CLASSIC queues with FIXED_DELAY_RETRYABLE_DISCARD strategy
        if (queueType == Management.QueueType.CLASSIC && RabbitMQConstants.ClassicDeadLetterStrategy.FIXED_DELAY_RETRYABLE_DISCARD.name().equalsIgnoreCase(deadLetterStrategy)) {
            configureFinalDeadLetterQueue(management, properties);
        }
    }

    /**
     * Configures the final dead-letter queue and exchange based on the provided properties.
     *
     * @param management The RabbitMQ management interface for queue operations.
     * @param properties The properties containing final dead-letter queue configurations.
     */
    private static void configureFinalDeadLetterQueue(Management management, Properties properties) {
        // Declare the final dead-letter queue if auto-declaration is enabled
        if (BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_QUEUE_AUTO_DECLARE)), false)) {
            String queueName = properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_QUEUE_NAME);
            Management.QueueType queueType = Management.QueueType.valueOf(
                    properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_QUEUE_TYPE, RabbitMQConstants.DEFAULT_QUEUE_TYPE));
            deadLetterQueueDeclare(management, queueName, queueType, null, null, null);
        }

        // Declare the final dead-letter exchange if auto-declaration is enabled
        if (properties.containsKey(RabbitMQConstants.FINAL_DEAD_LETTER_EXCHANGE_NAME)) {
            String exchangeName = properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_EXCHANGE_NAME);
            Management.ExchangeType exchangeType = Management.ExchangeType.valueOf(
                    properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_EXCHANGE_TYPE, RabbitMQConstants.DEFAULT_EXCHANGE_TYPE));

            if (BooleanUtils.toBooleanDefaultIfNull(
                    BooleanUtils.toBooleanObject(properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_EXCHANGE_AUTO_DECLARE)), false)) {
                deadLetterExchangeDeclare(management, exchangeName, exchangeType);
            }

            // Bind the exchange to the queue if the type is DIRECT
            if (exchangeType == Management.ExchangeType.DIRECT) {
                String queueName = properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_QUEUE_NAME);
                String routingKey = properties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_ROUTING_KEY, exchangeName);
                bindDeadLetterExchangeToQueue(management, queueName, exchangeName, routingKey);
            }
        }
    }

    /**
     * Declares a RabbitMQ exchange based on the provided properties.
     * This method supports various exchange types and allows optional arguments.
     *
     * @param management   The RabbitMQ management interface for exchange operations.
     * @param exchangeName The name of the exchange to be declared.
     * @param properties   The properties containing exchange configuration details.
     */
    public static void declareExchange(Management management, String exchangeName, Properties properties) {
        boolean autoDeclare = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(properties.getProperty(RabbitMQConstants.EXCHANGE_AUTODECLARE)), true);

        // Proceed only if the exchange name is valid, auto-declare is enabled, and the exchange is not a system exchange
        if (StringUtils.isNotEmpty(exchangeName) && autoDeclare && !exchangeName.startsWith("amq.")) {
            Management.ExchangeType exchangeType = Management.ExchangeType.valueOf(
                    properties.getProperty(RabbitMQConstants.EXCHANGE_TYPE, RabbitMQConstants.DEFAULT_EXCHANGE_TYPE));

            // Build the exchange specification
            Management.ExchangeSpecification exchangeBuilder = management.exchange()
                    .name(exchangeName)
                    .type(exchangeType)
                    .autoDelete(isAutoDeleteExchange(properties));

            // Add optional arguments if specified
            String arguments = properties.getProperty(RabbitMQConstants.EXCHANGE_ARGUMENTS);
            if (StringUtils.isNotEmpty(arguments)) {
                Map<String, Object> argumentMap = getOptionalArguments(properties, arguments);
                if (argumentMap != null) {
                    argumentMap.forEach(exchangeBuilder::argument);
                }
            }

            // Declare the exchange
            exchangeBuilder.declare();
        }
    }

    /**
     * Binds a RabbitMQ queue to an exchange based on the provided properties.
     * This method supports different exchange types such as HEADERS, FANOUT, DIRECT, and TOPIC.
     *
     * @param management   The RabbitMQ management interface for binding operations.
     * @param queueName    The name of the queue to bind.
     * @param exchangeName The name of the exchange to bind to.
     * @param properties   The properties containing binding configuration details.
     */
    public static void bindQueueToExchange(Management management, String queueName, String exchangeName, Properties properties) {
        // Validate that both the exchange name and queue name are provided
        if (StringUtils.isEmpty(exchangeName) || StringUtils.isEmpty(queueName)) {
            return;
        }

        // Build the binding specification
        Management.BindingSpecification bindingBuilder = management.binding()
                .sourceExchange(exchangeName)
                .destinationQueue(queueName);

        // Determine the exchange type and handle binding accordingly
        String exchangeType = properties.getProperty(RabbitMQConstants.EXCHANGE_TYPE, RabbitMQConstants.DEFAULT_EXCHANGE_TYPE);
        switch (Management.ExchangeType.valueOf(exchangeType)) {
            case HEADERS:
                handleHeadersExchange(bindingBuilder, queueName, exchangeName, properties);
                break;
            case FANOUT:
                bindingBuilder.bind(); // Directly bind for FANOUT exchanges
                break;
            default:
                handleDirectOrTopicExchange(bindingBuilder, queueName, properties); // Handle DIRECT or TOPIC exchanges
                break;
        }
    }

    /**
     * Handles the binding of a queue to a headers exchange.
     * This method processes the header arguments and binds the queue to the exchange
     * if valid arguments are provided.
     *
     * @param bindingBuilder The binding specification builder.
     * @param queueName      The name of the queue to bind.
     * @param exchangeName   The name of the headers exchange.
     * @param properties     The properties containing header arguments.
     */
    private static void handleHeadersExchange(Management.BindingSpecification bindingBuilder, String queueName, String exchangeName, Properties properties) {
        String headerArguments = properties.getProperty(RabbitMQConstants.HEADER_EXCHANGE_ARGUMENTS);

        // Return if no header arguments are specified
        if (StringUtils.isEmpty(headerArguments)) {
            if(log.isDebugEnabled()) {
                log.debug("No header arguments specified for the headers exchange. Hence not binding the queue: " + queueName + " to the exchange: " + exchangeName);
            }
            return;
        }

        // Parse header arguments into a map
        Map<String, Object> headerArgsMap = getOptionalArguments(properties, headerArguments);

        // Return if the parsed arguments are empty
        if (headerArgsMap == null || headerArgsMap.isEmpty()) {
            if(log.isDebugEnabled()) {
                log.debug("No header arguments specified for the headers exchange. Hence not binding the queue: " + queueName + " to the exchange: " + exchangeName);
            }
            return;
        }

        // Add default "x-match" argument if not present
        headerArgsMap.putIfAbsent("x-match", "all");

        // Bind the queue to the exchange if there are valid arguments
        headerArgsMap.forEach(bindingBuilder::argument);
        bindingBuilder.bind();
    }

    /**
     * Handles the binding of a queue to a direct or topic exchange.
     * This method uses the routing key from the properties or defaults to the queue name.
     *
     * @param bindingBuilder The binding specification builder.
     * @param queueName      The name of the queue to bind.
     * @param properties     The properties containing the routing key.
     */
    private static void handleDirectOrTopicExchange(Management.BindingSpecification bindingBuilder, String queueName, Properties properties) {
        // Use the routing key from properties or default to the queue name
        String routingKey = properties.getProperty(RabbitMQConstants.ROUTING_KEY, queueName);
        bindingBuilder.key(routingKey).bind();
    }


    /**
     * Checks if the queue is exclusive based on the provided properties.
     *
     * @param properties The properties containing the exclusive flag.
     * @return True if the queue is exclusive, false otherwise.
     */
    public static boolean isExclusiveQueue(Properties properties) {
        return BooleanUtils.toBoolean(properties.getProperty(RabbitMQConstants.QUEUE_EXCLUSIVE, "false"));
    }

    /**
     * Checks if the queue is auto-delete based on the provided properties.
     *
     * @param properties The properties containing the auto-delete flag.
     * @return True if the queue is auto-delete, false otherwise.
     */
    public static boolean isAutoDeleteQueue(Properties properties) {
        return BooleanUtils.toBoolean(properties.getProperty(RabbitMQConstants.QUEUE_AUTO_DELETE, "false"));
    }

    /**
     * Checks if the exchange is auto-delete based on the provided properties.
     *
     * @param properties The properties containing the auto-delete flag.
     * @return True if the exchange is auto-delete, false otherwise.
     */
    public static boolean isAutoDeleteExchange(Properties properties) {
        return BooleanUtils.toBoolean(properties.getProperty(RabbitMQConstants.EXCHANGE_AUTO_DELETE, "false"));
    }

    /**
     * Parses optional arguments from a comma-separated string into a map.
     *
     * @param properties The properties containing the arguments.
     * @param arguments  The comma-separated string of arguments.
     * @return A map of parsed arguments, or null if no valid arguments are found.
     */
    private static Map<String, Object> getOptionalArguments(String arguments) {
        Map<String, Object> optionalArgs = new HashMap<>();
        String[] keyValuePairs = arguments.split(",");

        // Parse each key-value pair and add to the map
        for (String pair : keyValuePairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                optionalArgs.put(keyValue[0].trim(), parseArgumentValue(keyValue[1].trim()));
            }
        }

        return optionalArgs.isEmpty() ? null : optionalArgs;
    }

    /**
     * Parses a string value into its appropriate data type.
     * Supports Boolean, Integer, Double, and defaults to String if parsing fails.
     *
     * @param value The string value to parse.
     * @return The parsed value as an Object.
     */
    private static Object parseArgumentValue(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value); // Parse as Boolean
        }
        try {
            return Integer.parseInt(value); // Try parsing as Integer
        } catch (NumberFormatException e) {
            try {
                return Double.parseDouble(value); // Try parsing as Double
            } catch (NumberFormatException ex) {
                return value; // Default to String
            }
        }
    }

    /**
     * Declares a dead-letter queue with the specified properties.
     *
     * @param management       The RabbitMQ management interface.
     * @param queueName        The name of the dead-letter queue.
     * @param queueType        The type of the queue (e.g., CLASSIC, QUORUM, STREAM).
     * @param messageTTL       The message TTL (time-to-live) in milliseconds, or null if not applicable.
     * @param mainExchangeName The name of the main exchange for dead-letter routing, or null if not applicable.
     * @param mainRoutingKey   The routing key for dead-letter routing, or null if not applicable.
     */
    private static void deadLetterQueueDeclare(Management management, String queueName, Management.QueueType queueType,
                                               Long messageTTL, String mainExchangeName, String mainRoutingKey) {
        if (queueName != null) {
            Management.QueueSpecification builder = management.queue()
                    .name(queueName)
                    .type(queueType);

            // Set message TTL if provided
            if (messageTTL != null && messageTTL > 0) {
                builder.messageTtl(Duration.ofMillis(messageTTL));
            }

            // Set dead-letter exchange and routing key if provided
            if (mainExchangeName != null) {
                builder.deadLetterExchange(mainExchangeName);
            }
            if (mainRoutingKey != null) {
                builder.deadLetterRoutingKey(mainRoutingKey);
            }

            builder.declare(); // Declare the queue
        }
    }

    /**
     * Declares a dead-letter exchange with the specified properties.
     *
     * @param management   The RabbitMQ management interface.
     * @param exchangeName The name of the dead-letter exchange.
     * @param exchangeType The type of the exchange (e.g., DIRECT, FANOUT, TOPIC).
     */
    private static void deadLetterExchangeDeclare(Management management, String exchangeName, Management.ExchangeType exchangeType) {
        if (exchangeName != null) {
            management.exchange(exchangeName)
                    .type(exchangeType)
                    .declare(); // Declare the exchange
        }
    }

    /**
     * Binds a dead-letter exchange to a queue with the specified routing key.
     *
     * @param management   The RabbitMQ management interface.
     * @param queueName    The name of the queue to bind.
     * @param exchangeName The name of the exchange to bind to.
     * @param routingKey   The routing key for the binding, or null if not applicable.
     */
    private static void bindDeadLetterExchangeToQueue(Management management, String queueName, String exchangeName, String routingKey) {
        if (queueName != null && exchangeName != null) {
            Management.BindingSpecification bindingSpecification = management.binding()
                    .sourceExchange(exchangeName)
                    .destinationQueue(queueName);

            // Set routing key if provided
            if (routingKey != null) {
                bindingSpecification.key(routingKey);
            }

            bindingSpecification.bind(); // Bind the exchange to the queue
        }
    }


}
