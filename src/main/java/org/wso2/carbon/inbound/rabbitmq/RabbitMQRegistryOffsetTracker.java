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
import org.apache.synapse.config.Entry;
import org.apache.synapse.registry.AbstractRegistry;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for tracking and managing RabbitMQ stream offsets in memory and in the registry.
 * It periodically flushes the current offset to the registry and
 * provides methods to store, retrieve, and update offsets.
 */
public class RabbitMQRegistryOffsetTracker {
    private static final Log log = LogFactory.getLog(RabbitMQRegistryOffsetTracker.class);
    private final AbstractRegistry registry;
    private final String registryPath;
    private final String resourcePath;
    private final String inboundName;
    private final Properties rabbitMqProperties;
    private long previousOffset = -1; // Initialize with an invalid offset value
    private final AtomicLong currentOffset = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * Constructor for RabbitMQRegistryOffsetTracker.
     *
     * @param registry           The registry instance to store offsets.
     * @param rabbitMqProperties The RabbitMQ properties.
     * @param name               The name of the resource in the registry.
     */
    public RabbitMQRegistryOffsetTracker(AbstractRegistry registry,
                                         Properties rabbitMqProperties, String name) {
        this.registry = registry;
        this.rabbitMqProperties = rabbitMqProperties;
        this.registryPath = RabbitMQConstants.REGISTRY_PATH + "/" + name;
        this.resourcePath = registryPath + "/" + RabbitMQConstants.RESOURCE_NAME;
        this.inboundName = name;

        // Determine the flush interval from properties or use the default value
        long interval;
        String intervalStr = rabbitMqProperties.getProperty(RabbitMQConstants.STREAM_OFFSET_TRACKER_FLUSH_INTERVAL);
        if (intervalStr != null && !intervalStr.trim().isEmpty()) {
            try {
                interval = Long.parseLong(intervalStr.trim());
            } catch (NumberFormatException e) {
                log.warn("[" + inboundName + "] Invalid value for "
                        + RabbitMQConstants.STREAM_OFFSET_TRACKER_FLUSH_INTERVAL
                        + ": '" + intervalStr + "'. Using default: "
                        + RabbitMQConstants.DEFAULT_STREAM_OFFSET_TRACKER_FLUSH_INTERVAL, e);
                interval = RabbitMQConstants.DEFAULT_STREAM_OFFSET_TRACKER_FLUSH_INTERVAL;
            }
        } else {
            interval = RabbitMQConstants.DEFAULT_STREAM_OFFSET_TRACKER_FLUSH_INTERVAL;
        }

        // Schedule periodic offset flushing
        scheduler.scheduleAtFixedRate(this::flushOffset, 10, interval, TimeUnit.SECONDS);
    }

    /**
     * Stores the given offset in memory.
     *
     * @param offset The offset to store.
     */
    public void storeOffsetInMemory(long offset) {
        currentOffset.set(offset);
    }

    /**
     * Retrieves the current offset stored in memory.
     *
     * @return The current offset.
     */
    public long getCurrentOffset() {
        return currentOffset.get();
    }

    /**
     * Flushes the current offset to the registry if it is greater than zero.
     */
    private void flushOffset() {
        long offset = currentOffset.get();
        if (offset > 0) {
            updateOffset(offset);
        }
    }

    /**
     * Updates the offset in the registry if it has changed since the last update.
     *
     * @param offset The offset to update.
     */
    private void updateOffset(long offset) {
        // Skip update if the offset hasn't changed or the registry is unavailable
        if (offset == previousOffset || registry == null) {
            return;
        }

        // Check if the registry resource exists
        Object registryResource = registry.getResource(new Entry(resourcePath), null);
        if (registryResource == null) {
            log.info("[" + inboundName + "] Registry resource not found. Creating new resource: " + resourcePath);
            registry.newResource(registryPath, true);
        }

        // Update the registry with the new offset
        registry.newNonEmptyResource(
                resourcePath,
                false,
                "text/plain",
                String.valueOf(offset), RabbitMQConstants.PROPERTY_NAME
        );
        previousOffset = offset;
    }

    /**
     * Loads the offset from the registry or returns the default offset strategy if unavailable.
     *
     * @return The loaded offset or the default offset strategy.
     */
    public Object loadOffset() {
        // Determine the offset strategy from properties or use the default strategy
        ConsumerBuilder.StreamOffsetSpecification offsetStrategy = ConsumerBuilder.StreamOffsetSpecification.valueOf(
                (String) rabbitMqProperties.getOrDefault(RabbitMQConstants.STREAM_OFFSET_STARTING_STRATEGY, "NEXT"));

        // Return the default strategy if the registry is unavailable
        if (registry == null) {
            log.warn("[" + inboundName + "] Registry is not available. " +
                    "Message consuming starts based on the consumer strategy: " + offsetStrategy);
            return offsetStrategy;
        }

        // Retrieve the registry resource
        Object registryResource = registry.getResource(new Entry(resourcePath), null);
        if (registryResource == null) {
            log.warn("[" + inboundName + "] Offset is not specified in the registry. " +
                    "Message consuming starts based on the consumer strategy: " + offsetStrategy);
            return offsetStrategy;
        }

        // Retrieve the offset from the registry resource properties
        Properties resourceProperties = registry.getResourceProperties(resourcePath);
        if (resourceProperties == null) {
            return offsetStrategy;
        }

        String offsetStr = resourceProperties.getProperty(RabbitMQConstants.PROPERTY_NAME);
        try {
            return Long.parseLong(offsetStr);
        } catch (NumberFormatException e) {
            log.warn("[" + inboundName + "] Offset in the registry is not a valid number." +
                    " Message consuming starts based on the consumer strategy: " + offsetStrategy);
            return offsetStrategy;
        }
    }

    /**
     * Shuts down the tracker, ensuring the scheduler is terminated and the offset is flushed.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            // Determine the shutdown timeout from properties or use the default value
            long timeout;
            String timeoutStr = rabbitMqProperties
                    .getProperty(RabbitMQConstants.STREAM_OFFSET_TRACKER_SHUTDOWN_TIMEOUT);
            if (timeoutStr != null) {
                try {
                    timeout = Long.parseLong(timeoutStr);
                } catch (NumberFormatException e) {
                    log.warn("[" + inboundName + "] Shutdown timeout property is not a valid number."
                            + " Using default value: "
                            + RabbitMQConstants.DEFAULT_STREAM_OFFSET_TRACKER_SHUTDOWN_TIMEOUT
                    );
                    timeout = RabbitMQConstants.DEFAULT_STREAM_OFFSET_TRACKER_SHUTDOWN_TIMEOUT;
                }
            } else {
                timeout = RabbitMQConstants.DEFAULT_STREAM_OFFSET_TRACKER_SHUTDOWN_TIMEOUT;
            }

            // Await termination of the scheduler
            if (!scheduler.awaitTermination(timeout, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Flush the offset one last time
        flushOffset();
    }
}
