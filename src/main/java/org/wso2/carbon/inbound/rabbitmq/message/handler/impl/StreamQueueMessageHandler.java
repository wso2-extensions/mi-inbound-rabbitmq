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
package org.wso2.carbon.inbound.rabbitmq.message.handler.impl;

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;

import org.apache.axis2.context.MessageContext;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQAcknowledgementMode;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQConstants;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQMessageContext;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQRegistryOffsetTracker;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQRoundRobinAddressSelector;
import org.wso2.carbon.inbound.rabbitmq.message.handler.AbstractRabbitMQMessageHandler;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

/**
 * This class handles RabbitMQ messages from stream queues. It extends the
 * AbstractRabbitMQMessageHandler to provide specific handling for stream queues,
 * including message filtering, offset tracking, and acknowledgment handling.
 */
public class StreamQueueMessageHandler extends AbstractRabbitMQMessageHandler {
    private static final Log log = LogFactory.getLog(StreamQueueMessageHandler.class);
    private final RabbitMQRegistryOffsetTracker registryOffsetTracker;


    /**
     * Constructor for StreamQueueMessageHandler.
     * Initializes the handler with the provided parameters and ensures sequential processing for stream queues.
     *
     * @param inboundName           The name of the inbound endpoint.
     * @param injectingSeq          The sequence to inject messages into.
     * @param onErrorSeq            The sequence to handle errors.
     * @param sequential            Whether the processing is sequential.
     * @param synapseEnvironment    The Synapse environment.
     * @param rabbitMQProperties    The RabbitMQ properties.
     * @param addressSelector       The address selector for RabbitMQ.
     * @param registryOffsetTracker The offset tracker for the registry.
     */
    public StreamQueueMessageHandler(String inboundName, String injectingSeq, String onErrorSeq, boolean sequential,
                                     SynapseEnvironment synapseEnvironment, Properties rabbitMQProperties,
                                     RabbitMQRoundRobinAddressSelector addressSelector,
                                     RabbitMQRegistryOffsetTracker registryOffsetTracker) {
        super(inboundName, injectingSeq, onErrorSeq, true, synapseEnvironment, rabbitMQProperties, addressSelector);

        // Log a warning if sequential processing is disabled, as stream queues only support sequential processing
        if (!sequential) {
            log.warn("[" + inboundName + "] Stream queues only support sequential processing. " +
                    "Hence changing the 'sequential' property to true for the inbound endpoint: " + inboundName);
        }

        // Initialize the registry offset tracker
        this.registryOffsetTracker = registryOffsetTracker;
    }

    /**
     * Handles incoming RabbitMQ messages for stream queues.
     * Filters messages based on stream filters and processes them sequentially.
     *
     * @param consumerContext The RabbitMQ consumer context.
     * @param message         The RabbitMQ message.
     */
    @Override
    public void handle(Consumer.Context consumerContext, Message message) {

        if (isThrottlingEnabled) {
            handleThrottling();
        }
        // Retrieve the current RabbitMQ address
        Address address = addressSelector.getCurrentAddress();

        RabbitMQAcknowledgementMode acknowledgementMode;

        // Create a RabbitMQ message context
        RabbitMQMessageContext rabbitMQMsgCtx = new RabbitMQMessageContext(message, address.host(),
                address.port(), this.queue);

        // Create a Synapse message context
        org.apache.synapse.MessageContext synapseMsgCtx = createMessageContext();
        MessageContext axis2MsgCtx = ((Axis2MessageContext) synapseMsgCtx).getAxis2MessageContext();

        // Retrieve stream filter values and properties
        String filterValue = (String) message.annotation(RabbitMQConstants.STREAM_FILTER_VALUE_ANNOTATION);
        String streamFilters = rabbitMQProperties.getProperty(RabbitMQConstants.STREAM_FILTERS);
        boolean matchUnfiltered = BooleanUtils
                .toBoolean(rabbitMQProperties.getProperty(RabbitMQConstants.STREAM_FILTER_MATCH_UNFILTERED));

        // Check if stream filters are defined
        if (StringUtils.isNotEmpty(streamFilters)) {
            String[] filterArray = streamFilters.split(",");

            // Process the message if it matches the filters or unfiltered messages are allowed
            if (matchUnfiltered || Arrays.asList(filterArray).contains(filterValue)) {

                // Process the message and store the offset
                acknowledgementMode = onMessage(synapseMsgCtx, axis2MsgCtx, rabbitMQMsgCtx, inboundName);
                long offset = (long) message.annotation(RabbitMQConstants.X_STREAM_OFFSET);
                registryOffsetTracker.storeOffsetInMemory(offset);

                // Handle message acknowledgment
                handleAcknowledgement(axis2MsgCtx, rabbitMQMsgCtx, consumerContext, acknowledgementMode);
            } else if (log.isDebugEnabled()) {
                log.debug("[" + inboundName + "] Message filtered out by stream filter. Filter Value: " + filterValue);
            }
        } else {
            // Process the message and store the offset if no filters are defined
            acknowledgementMode = onMessage(synapseMsgCtx, axis2MsgCtx, rabbitMQMsgCtx, inboundName);
            long offset = (long) message.annotation(RabbitMQConstants.X_STREAM_OFFSET);
            registryOffsetTracker.storeOffsetInMemory(offset);

            // Handle message acknowledgment
            handleAcknowledgement(axis2MsgCtx, rabbitMQMsgCtx, consumerContext, acknowledgementMode);
        }
    }

    /**
     * Handles message acknowledgment for stream queues.
     * Ensures that unsupported acknowledgment modes are logged and handled appropriately.
     *
     * @param axis2MsgCtx         The Axis2 message context.
     * @param rabbitMQMsgCtx      The RabbitMQ message context.
     * @param context             The RabbitMQ consumer context.
     * @param acknowledgementMode The acknowledgment mode.
     */
    @Override
    protected void handleAcknowledgement(MessageContext axis2MsgCtx, RabbitMQMessageContext rabbitMQMsgCtx,
                                         Consumer.Context context, RabbitMQAcknowledgementMode acknowledgementMode) {
        switch (Objects.requireNonNull(acknowledgementMode)) {
            case ACCEPTED:
                // Accept the message
                context.accept();
                break;
            case REQUEUE:
                // Log a warning and accept the message, as requeue is not supported for stream queues
                log.warn("[" + inboundName + "] Requeue is not supported with the STREAM Type queues, " +
                        "hence accepting the message and moving forward. " +
                        "If you want to retrieve the failure message again " +
                        "please configure the offset to current offset: " + registryOffsetTracker.getCurrentOffset() +
                        " to and redeploy the inbound endpoint.");
                context.accept();
                break;
            case DISCARDED:
                // Log a warning and accept the message, as discard is not supported for stream queues
                log.warn("[" + inboundName + "] Discard is not supported with the STREAM Type queues, " +
                        "hence accepting the message and moving forward. " +
                        "If you want to retrieve the failure message again " +
                        "please configure the offset to current offset: " + registryOffsetTracker.getCurrentOffset() +
                        "to and redeploy the inbound endpoint.");
                context.accept();
                break;
            default:
                // Log a warning for unknown acknowledgment modes
                log.warn("[" + inboundName + "] Unknown AcknowledgementMode: " + acknowledgementMode);
                // Nothing to do here
        }
    }

    /**
     * Shuts down the StreamQueueMessageHandler.
     * Releases resources and shuts down the registry offset tracker.
     */
    @Override
    public void shutdown() {
        registryOffsetTracker.shutdown();
    }
}
