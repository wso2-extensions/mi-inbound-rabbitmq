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
package org.wso2.carbon.inbound.rabbitmq.message.handler;


import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.base.AckDecision;
import org.apache.axis2.transport.base.AckDecisionCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQAcknowledgementMode;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQConstants;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQMessageContext;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQRoundRobinAddressSelector;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQUtils;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Abstract base class for handling RabbitMQ messages.
 * Provides common functionality for processing, acknowledging, and managing RabbitMQ messages.
 * Subclasses must implement specific message handling and acknowledgment logic.
 */
public abstract class AbstractRabbitMQMessageHandler implements Consumer.MessageHandler {

    private static final Log log = LogFactory.getLog(AbstractRabbitMQMessageHandler.class);
    protected final String inboundName;
    protected final String injectingSeq;
    protected final String onErrorSeq;
    protected final boolean sequential;
    protected final SynapseEnvironment synapseEnvironment;
    protected final SequenceMediator seq;
    protected final String queue;
    protected final RabbitMQRoundRobinAddressSelector addressSelector;
    protected final Properties rabbitMQProperties;

    // Constructor to initialize the AbstractRabbitMQMessageHandler with necessary configurations
    public AbstractRabbitMQMessageHandler(String inboundName, String injectingSeq,
                                          String onErrorSeq, boolean sequential,
                                          SynapseEnvironment synapseEnvironment,
                                          Properties rabbitMQProperties,
                                          RabbitMQRoundRobinAddressSelector addressSelector) {
        this.inboundName = inboundName; // Set the inbound endpoint name
        this.injectingSeq = injectingSeq; // Set the injecting sequence name

        // Validate that the injecting sequence is specified
        if (injectingSeq == null || injectingSeq.isEmpty()) {
            // Error message for missing injecting sequence
            String msg = "[" + inboundName + "] Injecting Sequence name is not specified.";
            log.error(msg); // Log the error
            throw new SynapseException(msg); // Throw an exception
        }

        // Retrieve and initialize the injecting sequence
        seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration().getSequence(injectingSeq);
        if (seq == null) {
            throw new SynapseException("Specified injecting sequence: " + injectingSeq + "is invalid.");
        }
        if (!seq.isInitialized()) {
            // Initialize the sequence if not already initialized
            seq.init(synapseEnvironment);
        }

        this.onErrorSeq = onErrorSeq;
        this.sequential = sequential;
        this.synapseEnvironment = synapseEnvironment;
        this.addressSelector = addressSelector;
        this.rabbitMQProperties = rabbitMQProperties;

        // Retrieve the queue name from RabbitMQ properties
        this.queue = rabbitMQProperties.getProperty(RabbitMQConstants.QUEUE_NAME);
    }


    /**
     * Handles the incoming RabbitMQ message by determining the message builder,
     * setting the message payload to the message context, and injecting the message.
     *
     * @param synapseMsgCtx          The Synapse message context.
     * @param axis2MsgCtx            The Axis2 message context.
     * @param rabbitMQMessageContext The RabbitMQ message context.
     * @param inboundName            The name of the inbound endpoint.
     * @return The delivery status of the message.
     */
    public RabbitMQAcknowledgementMode onMessage(org.apache.synapse.MessageContext synapseMsgCtx,
                                                 MessageContext axis2MsgCtx,
                                                 RabbitMQMessageContext rabbitMQMessageContext,
                                                 String inboundName) {
        try {
            // Build the RabbitMQ message and set transport headers
            RabbitMQUtils.buildMessage(rabbitMQMessageContext, axis2MsgCtx, rabbitMQProperties);
            axis2MsgCtx.setProperty(MessageContext.TRANSPORT_HEADERS,
                    RabbitMQUtils.getTransportHeaders(rabbitMQMessageContext, axis2MsgCtx));

            AckDecisionCallback ackDecisionCallback = new AckDecisionCallback();

            // Inject the message into the specified sequence
            if (seq != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[" + inboundName + "] injecting message to sequence : " + injectingSeq);
                }
                seq.setErrorHandler(onErrorSeq);
                synapseMsgCtx.setProperty(SynapseConstants.IS_INBOUND, true);
                synapseMsgCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, inboundName);
                synapseMsgCtx.setProperty(SynapseConstants.ARTIFACT_NAME,
                        SynapseConstants.FAIL_SAFE_MODE_INBOUND_ENDPOINT + inboundName);

                // Populate statistics metadata if statistics are enabled
                if (RuntimeStatisticCollector.isStatisticsEnabled()) {
                    populateStatisticsMetadata(synapseMsgCtx, rabbitMQMessageContext);
                }

                // Set acknowledgment decision callback and inject the message
                synapseMsgCtx.setProperty(RabbitMQConstants.ACKNOWLEDGEMENT_DECISION, ackDecisionCallback);
                synapseEnvironment.injectInbound(synapseMsgCtx, seq, sequential);
            } else {
                log.error("[" + inboundName + "] Sequence: " + injectingSeq + " not found");
            }

            boolean autoAck = "true".equalsIgnoreCase(
                    (String) rabbitMQProperties.getOrDefault(RabbitMQConstants.AUTO_ACK_ENABLED, "false"));

            if (autoAck) {
                return RabbitMQAcknowledgementMode.ACCEPTED;
            }

            AckDecision ackDecision;
            try {
                // Wait for acknowledgment decision with a timeout
                long ackWaitTime;
                if (rabbitMQProperties.containsKey(RabbitMQConstants.ACK_MAX_WAIT_TIME)) {
                    String ackWaitTimeStr = rabbitMQProperties.getProperty(RabbitMQConstants.ACK_MAX_WAIT_TIME);
                    try {
                        ackWaitTime = Long.parseLong(ackWaitTimeStr);
                    } catch (NumberFormatException e) {
                        log.warn("[" + inboundName + "] Invalid value for "
                                + RabbitMQConstants.ACK_MAX_WAIT_TIME + " : '" + ackWaitTimeStr
                                + "'. Using default value: " + RabbitMQConstants.DEFAULT_ACK_MAX_WAIT_TIME, e);
                        ackWaitTime = RabbitMQConstants.DEFAULT_ACK_MAX_WAIT_TIME;
                    }
                } else {
                    ackWaitTime = RabbitMQConstants.DEFAULT_ACK_MAX_WAIT_TIME;
                }
                ackDecision = ackDecisionCallback.await(ackWaitTime);
            } catch (InterruptedException e) {
                // Handle thread interruption during acknowledgment wait
                log.warn("[" + inboundName + "] Thread interrupted while waiting for ACK decision from mediation."
                        + " Setting to default REQUEUE_ON_ROLLBACK");
                ackDecision = AckDecision.SET_REQUEUE_ON_ROLLBACK;
            }

            // Fallback to default acknowledgment decision if none is received
            if (ackDecision == null) {
                log.warn("[" + inboundName + "] Timeout while waiting for ACK decision from mediation Setting to"
                        + " default REQUEUE_ON_ROLLBACK");
                ackDecision = AckDecision.SET_REQUEUE_ON_ROLLBACK;
            }

            // Set the acknowledgment decision property in the message context
            setAckDecisionProperty(ackDecision, synapseMsgCtx);

            // Return the acknowledgment mode based on the message context properties
            return getAcknowledgementMode(synapseMsgCtx);

        } catch (AxisFault axisFault) {
            // Log and handle errors during message processing
            log.error("[" + inboundName + "] Error when trying to read incoming message ...", axisFault);
            return RabbitMQAcknowledgementMode.DISCARDED;
        }
    }

    /**
     * Creates the initial message context for RabbitMQ.
     *
     * @return The initialized Synapse message context.
     */
    protected org.apache.synapse.MessageContext createMessageContext() {
        // Create a new Synapse message context
        org.apache.synapse.MessageContext msgCtx = synapseEnvironment.createMessageContext();

        // Set Axis2 message context properties
        MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.Axis2MessageContext) msgCtx)
                .getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUID.randomUUID().toString());

        // Set client API non-blocking property
        msgCtx.setProperty(MessageContext.CLIENT_API_NON_BLOCKING, true);

        return msgCtx;
    }

    /**
     * Populates the statistics metadata for the given Synapse message context.
     *
     * @param synCtx                 The Synapse message context.
     * @param rabbitMQMessageContext The RabbitMQ message context.
     */
    protected void populateStatisticsMetadata(org.apache.synapse.MessageContext synCtx,
                                              RabbitMQMessageContext rabbitMQMessageContext) {
        // Populate statistics metadata with RabbitMQ message details
        Map<String, Object> statisticsDetails = new HashMap<>();
        statisticsDetails.put(SynapseConstants.HOSTNAME, rabbitMQMessageContext.getHost());
        statisticsDetails.put(SynapseConstants.PORT, rabbitMQMessageContext.getPort());
        statisticsDetails.put(SynapseConstants.QUEUE, rabbitMQMessageContext.getQueue());
        synCtx.setProperty(SynapseConstants.STATISTICS_METADATA, statisticsDetails);
    }


    /**
     * Handles the incoming RabbitMQ message by processing it and determining the acknowledgment mode.
     *
     * @param consumerContext The RabbitMQ consumer context.
     * @param message         The RabbitMQ message to be processed.
     */
    @Override
    public abstract void handle(Consumer.Context consumerContext, Message message);


    /**
     * Sets the acknowledgment decision property in the message context.
     *
     * @param ackDecision The acknowledgment decision.
     * @param msgContext  The Synapse message context.
     */
    protected void setAckDecisionProperty(AckDecision ackDecision,
                                          org.apache.synapse.MessageContext msgContext) {
        // Retrieve the acknowledgment mode as a string value
        String acknowledgementMode = ackDecision.toStringValue();

        // Clear the acknowledgment decision property in the message context
        msgContext.setProperty(RabbitMQConstants.ACKNOWLEDGEMENT_DECISION, null);

        // Set specific properties based on the acknowledgment mode
        switch (acknowledgementMode) {
            case RabbitMQConstants.ACKNOWLEDGE:
                // No action needed for ACKNOWLEDGE
                break;
            case RabbitMQConstants.SET_ROLLBACK_ONLY:
                // Set the rollback-only property to true
                msgContext.setProperty(RabbitMQConstants.SET_ROLLBACK_ONLY, Boolean.TRUE);
                break;
            case RabbitMQConstants.SET_REQUEUE_ON_ROLLBACK:
                // Set the requeue-on-rollback property to true
                msgContext.setProperty(RabbitMQConstants.SET_REQUEUE_ON_ROLLBACK, Boolean.TRUE);
                break;
            default:
                // Do nothing for unrecognized acknowledgment modes
        }
    }


    /**
     * Retrieves the acknowledgment mode based on the message context properties.
     *
     * @param msgContext The Synapse message context.
     * @return The acknowledgment mode.
     */
    protected RabbitMQAcknowledgementMode getAcknowledgementMode(org.apache.synapse.MessageContext msgContext) {
        RabbitMQAcknowledgementMode acknowledgementMode;
        // Check if the property `SET_ROLLBACK_ONLY` is set, and if so, set the mode to DISCARDED
        if (isBooleanPropertySet(RabbitMQConstants.SET_ROLLBACK_ONLY, msgContext)) {
            acknowledgementMode = RabbitMQAcknowledgementMode.DISCARDED;
            // Check if the property `SET_REQUEUE_ON_ROLLBACK` is set, and if so, set the mode to REQUEUE
        } else if (isBooleanPropertySet(RabbitMQConstants.SET_REQUEUE_ON_ROLLBACK, msgContext)) {
            acknowledgementMode = RabbitMQAcknowledgementMode.REQUEUE;
            // Default to ACCEPTED if no other properties are set
        } else {
            acknowledgementMode = RabbitMQAcknowledgementMode.ACCEPTED;
        }
        return acknowledgementMode; // Return the determined acknowledgment mode
    }

    /**
     * Checks if a boolean property is set in the message context.
     *
     * @param propertyName   The name of the property.
     * @param messageContext The Synapse message context.
     * @return True if the property is set, false otherwise.
     */
    protected boolean isBooleanPropertySet(String propertyName,
                                           org.apache.synapse.MessageContext messageContext) {
        // Retrieve the property value from the message context
        Object property = messageContext.getProperty(propertyName);
        // Check if the property is a Boolean and true, or a String that can be parsed as true
        return (property instanceof Boolean && ((Boolean) property)) ||
                (property instanceof String && Boolean.parseBoolean((String) property));
    }

    /**
     * Handles the acknowledgment for the RabbitMQ message.
     *
     * @param axis2MsgCtx         The Axis2 message context.
     * @param rabbitMQMsgCtx      The RabbitMQ message context.
     * @param context             The RabbitMQ consumer context.
     * @param acknowledgementMode The acknowledgment mode.
     */
    protected abstract void handleAcknowledgement(MessageContext axis2MsgCtx,
                                                  RabbitMQMessageContext rabbitMQMsgCtx,
                                                  Consumer.Context context,
                                                  RabbitMQAcknowledgementMode acknowledgementMode);


    /**
     * Clones the RabbitMQMessageContext and creates a new Message object.
     *
     * @param originalContext         The original RabbitMQMessageContext to clone.
     * @param newMessage              The new Message object to populate.
     * @param includeXDeathAnnotation Whether to include the X-Death annotation.
     * @return A new Message object with the cloned properties and annotations.
     */

    protected Message cloneMessageWithContext(RabbitMQMessageContext originalContext,
                                              Message newMessage,
                                              boolean includeXDeathAnnotation) {

        // Copy annotations from the original context to the new message
        if (originalContext.hasAnnotations()) {
            originalContext.getAnnotations().forEach((key, value) -> {
                // Include the annotation unless it's the X-Death annotation and includeXDeathAnnotation is false
                if (includeXDeathAnnotation || !RabbitMQConstants.X_OPT_DEATHS.equals(key)) {
                    newMessage.annotation(key, value);
                }
            });
        }

        // Copy application properties from the original context to the new message
        if (originalContext.hasProperties()) {
            originalContext.getApplicationProperties().forEach((key, value) -> {
                // Set the property in the new message based on its type
                if (value instanceof Boolean) {
                    newMessage.property(key, (Boolean) value);
                } else if (value instanceof Integer) {
                    newMessage.property(key, (Integer) value);
                } else if (value instanceof Long) {
                    newMessage.property(key, (Long) value);
                } else if (value instanceof Double) {
                    newMessage.property(key, (Double) value);
                } else if (value instanceof String) {
                    newMessage.property(key, (String) value);
                } else if (value instanceof Byte) {
                    newMessage.property(key, (Byte) value);
                } else if (value instanceof Float) {
                    newMessage.property(key, (Float) value);
                } else if (value instanceof UUID) {
                    newMessage.property(key, (UUID) value);
                } else if (value instanceof Short) {
                    newMessage.property(key, (Short) value);
                }
            });
        }

        // Set additional properties in the new message
        newMessage.correlationId(originalContext.getCorrelationId());
        newMessage.replyTo(originalContext.getReplyTo());
        newMessage.contentType(originalContext.getContentType());
        newMessage.contentEncoding(originalContext.getContentEncoding());

        return newMessage;
    }


    /**
     * Shuts down the message handler and releases any resources held by it.
     */
    public abstract void shutdown();


}
