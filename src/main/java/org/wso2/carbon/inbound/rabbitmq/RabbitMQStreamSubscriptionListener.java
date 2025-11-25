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

import com.rabbitmq.client.amqp.ConsumerBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import java.util.Properties;

/**
 * This class implements the `ConsumerBuilder.SubscriptionListener` interface to handle
 * RabbitMQ stream subscription events. It manages the subscription process, including
 * setting the stream offset based on the provided RabbitMQ properties or the registry offset tracker.
 */
public class RabbitMQStreamSubscriptionListener implements ConsumerBuilder.SubscriptionListener {

    private static final Log log = LogFactory.getLog(RabbitMQStreamSubscriptionListener.class);
    private final RabbitMQRegistryOffsetTracker registryOffsetTracker;
    private final Properties rabbitMqProperties;
    /**
     * Constructor for RabbitMQStreamSubscriptionListener.
     *
     * @param registryOffsetTracker The tracker to manage registry offsets.
     * @param rabbitMqProperties    The RabbitMQ properties.
     */
    public RabbitMQStreamSubscriptionListener(RabbitMQRegistryOffsetTracker registryOffsetTracker,
                                              Properties rabbitMqProperties) {
        this.registryOffsetTracker = registryOffsetTracker;
        this.rabbitMqProperties = rabbitMqProperties;
    }

    /**
     * Prepares the subscription by setting the stream offset.
     *
     * @param context The subscription context.
     */
    @Override
    public void preSubscribe(Context context) {
        // Retrieve the starting offset from properties
        String startingOffset = rabbitMqProperties.getProperty(RabbitMQConstants.STREAM_OFFSET_STARTING_VALUE);

        if (startingOffset != null) {
            try {
                // Set the offset from the starting value
                context.streamOptions().offset(Long.parseLong(startingOffset));
            } catch (NumberFormatException e) {
                log.error("Invalid stream offset starting value provided", e);
                throw new SynapseException("Invalid stream offset starting value provided", e);
            }
        } else {
            // Load the offset from the registry and set it
            Object offset = registryOffsetTracker.loadOffset();
            if (offset instanceof Long) {
                context.streamOptions().offset((long) offset + 1);
            } else if (offset instanceof ConsumerBuilder.StreamOffsetSpecification) {
                context.streamOptions().offset((ConsumerBuilder.StreamOffsetSpecification) offset);
            }
        }
    }
}
