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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQAcknowledgementMode;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQConstants;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQMessageContext;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQRoundRobinAddressSelector;
import org.wso2.carbon.inbound.rabbitmq.message.handler.AbstractRabbitMQMessageHandler;


import java.util.Objects;
import java.util.Properties;



/**
 * Handles RabbitMQ messages for quorum queues.
 * Provides functionality for processing messages and managing acknowledgments.
 */
public class QuorumQueueMessageHandler extends AbstractRabbitMQMessageHandler {

    private static final Log log = LogFactory.getLog(QuorumQueueMessageHandler.class);
    private final String consumerID;

    /**
     * Constructor for QuorumQueueMessageHandler.
     * Initializes the handler with the provided parameters.
     *
     * @param inboundName        The name of the inbound endpoint.
     * @param injectingSeq       The sequence to inject messages into.
     * @param onErrorSeq         The sequence to handle errors.
     * @param sequential         Whether the processing is sequential.
     * @param synapseEnvironment The Synapse environment.
     * @param rabbitMQProperties The RabbitMQ properties.
     * @param addressSelector    The address selector for RabbitMQ.
     */
    public QuorumQueueMessageHandler(String inboundName, String injectingSeq, String onErrorSeq, boolean sequential,
                                     SynapseEnvironment synapseEnvironment, Properties rabbitMQProperties,
                                     RabbitMQRoundRobinAddressSelector addressSelector, String consumerID) {
        super(inboundName, injectingSeq, onErrorSeq, sequential,
                synapseEnvironment, rabbitMQProperties, addressSelector);
        this.consumerID = consumerID;
    }

    /**
     * Handles incoming RabbitMQ messages for quorum queues.
     * Processes the message and handles acknowledgment based on the acknowledgment mode.
     *
     * @param consumerContext The RabbitMQ consumer context.
     * @param message         The RabbitMQ message.
     */
    @Override
    public void handle(Consumer.Context consumerContext, Message message) {

        // Retrieve the current RabbitMQ address
        Address address = addressSelector.getCurrentAddress();

        RabbitMQAcknowledgementMode acknowledgementMode = null;

        // Create a RabbitMQ message context
        RabbitMQMessageContext rabbitMQMsgCtx = new RabbitMQMessageContext(message,
                address.host(), address.port(), this.queue);

        // Create a Synapse message context
        org.apache.synapse.MessageContext synapseMsgCtx = createMessageContext();
        MessageContext axis2MsgCtx = ((Axis2MessageContext) synapseMsgCtx).getAxis2MessageContext();

        // Process the message and determine the acknowledgment mode
        acknowledgementMode = onMessage(synapseMsgCtx, axis2MsgCtx, rabbitMQMsgCtx, inboundName);

        // Handle message acknowledgment
        handleAcknowledgement(axis2MsgCtx, rabbitMQMsgCtx, consumerContext, acknowledgementMode);
    }

    /**
     * Handles message acknowledgment for quorum queues.
     * Supports acknowledgment modes: ACCEPTED, REQUEUE, and DISCARDED.
     *
     * @param axis2MsgCtx         The Axis2 message context.
     * @param rabbitMQMsgCtx      The RabbitMQ message context.
     * @param context             The RabbitMQ consumer context.
     * @param acknowledgementMode The acknowledgment mode.
     */
    @Override
    protected void handleAcknowledgement(MessageContext axis2MsgCtx,
                                         RabbitMQMessageContext rabbitMQMsgCtx,
                                         Consumer.Context context,
                                         RabbitMQAcknowledgementMode acknowledgementMode) {

        // Retrieve the message ID for logging purposes
        String messageID = rabbitMQMsgCtx.getMessageID() != null ?
                rabbitMQMsgCtx.getMessageID() : axis2MsgCtx.getMessageID();

        // Handle acknowledgment based on the mode
        switch (Objects.requireNonNull(acknowledgementMode)) {
            case ACCEPTED:
                // Accept the message
                context.accept();
                break;
            case REQUEUE:
                // Delay requeue if the delay property is set
                if (rabbitMQProperties.containsKey(RabbitMQConstants.MESSAGE_REQUEUE_DELAY)) {
                    String delayValue = rabbitMQProperties.getProperty(RabbitMQConstants.MESSAGE_REQUEUE_DELAY);
                    try {
                        if (delayValue != null) {
                            long delay = Long.parseLong(delayValue);
                            Thread.sleep(delay);
                        } else {
                            log.warn("[" + inboundName + "][" + consumerID + "] Requeue delay value is null " +
                                    "for message id: " + messageID + ". Skipping delay.");
                        }
                    } catch (NumberFormatException ex) {
                        log.warn("[" + inboundName + "][" + consumerID + "] Invalid requeue delay value '"
                                + delayValue + "' for" + " message id: " + messageID + ". Skipping delay.", ex);
                    } catch (InterruptedException ex) {
                        log.warn("[" + inboundName + "][" + consumerID + "] Thread has been interrupted " +
                                "while delaying message requeue for message id: " + messageID, ex);
                    }
                }
                // Requeue the message
                context.requeue();
                break;
            case DISCARDED:
                // Discard the message
                context.discard();
                break;
            default:
                // Log a warning for unknown acknowledgment modes
                log.warn("[" + inboundName + "][" + consumerID + "] Unknown AcknowledgementMode: "
                        + acknowledgementMode);
                // Nothing to do here
        }
    }

    /**
     * Shuts down the QuorumQueueMessageHandler.
     * Releases resources if necessary.
     */
    @Override
    public void shutdown() {
        // No specific shutdown logic implemented
    }

}
