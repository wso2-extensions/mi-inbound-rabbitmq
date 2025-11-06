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

import com.rabbitmq.client.amqp.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Listener class for monitoring the state of RabbitMQ connections.
 * Logs state transitions such as opening, recovering, closing, and errors.
 */
public class RabbitMQConnectionStateListener implements Resource.StateListener {

    private static final Log log = LogFactory.getLog(RabbitMQConnectionStateListener.class);

    private final String inboundName;

    /**
     * Constructor for RabbitMQConnectionStateListener.
     *
     * @param inboundName The name of the inbound endpoint associated with this listener.
     */
    public RabbitMQConnectionStateListener(String inboundName) {
        this.inboundName = inboundName;
    }

    @Override
    /**
     * Handles state transitions of the RabbitMQ connection.
     *
     * @param context The context containing the current and previous states of the connection.
     */
    public void handle(Resource.Context context) {

        // Handle transition from OPEN to RECOVERING state
        if (context.currentState() == Resource.State.RECOVERING && context.previousState() == Resource.State.OPEN) {
            log.info("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker started to recover.");
        }

        // Handle transition from RECOVERING to OPEN state
        if (context.currentState() == Resource.State.OPEN && context.previousState() == Resource.State.RECOVERING) {
            log.info("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker was recovered.");
        }

        // Handle transition from null to OPENING state
        if (context.currentState() == Resource.State.OPENING && context.previousState() == null) {
            log.info("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker is opening.");
        }

        // Handle transition from OPENING to OPEN state
        if (context.currentState() == Resource.State.OPEN && context.previousState() == Resource.State.OPENING) {
            log.info("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker is established.");
        }

        // Handle transition from OPEN to CLOSING state
        if (context.currentState() == Resource.State.CLOSING && context.previousState() == Resource.State.OPEN) {
            if (context.failureCause() != null) {
                log.warn("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker is closing due to: " + context.failureCause());
            } else {
                log.info("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker is closing.");
            }
        }

        // Handle transition from CLOSING to CLOSED state
        if (context.currentState() == Resource.State.CLOSED && context.previousState() == Resource.State.CLOSING) {
            if (context.failureCause() == null) {
                log.info("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker is closed.");
            } else {
                log.warn("[" + inboundName + "] inbound endpoint connection to the RabbitMQ broker is closed due to: " + context.failureCause());
            }
        }
    }
}
