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
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Management;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.deployers.SynapseArtifactDeploymentException;
import org.apache.synapse.registry.AbstractRegistry;
import org.wso2.carbon.inbound.endpoint.protocol.PollingConstants;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumer;
import org.wso2.carbon.inbound.rabbitmq.message.handler.AbstractRabbitMQMessageHandler;
import org.wso2.carbon.inbound.rabbitmq.message.handler.impl.ClassicQueueMessageHandler;
import org.wso2.carbon.inbound.rabbitmq.message.handler.impl.QuorumQueueMessageHandler;
import org.wso2.carbon.inbound.rabbitmq.message.handler.impl.StreamQueueMessageHandler;


import java.util.Properties;

/**
 * This class represents a RabbitMQ listener that listens to RabbitMQ queues
 * and processes messages based on the specified queue type (STREAM, CLASSIC, or QUORUM).
 * It manages the RabbitMQ connection, consumer, and message handler lifecycle.
 */
public class RabbitMQListener extends GenericEventBasedConsumer {
    private static final Log log = LogFactory.getLog(RabbitMQListener.class);
    private final Properties rabbitmqProperties;
    private final SynapseEnvironment synapseEnvironment;
    private AbstractRabbitMQMessageHandler rabbitMqMessageHandler;
    private RabbitMQEnvironment environment = null;
    private Connection connection = null;
    private Consumer rabbitMQConsumer = null;
    private RabbitMQRegistryOffsetTracker registryOffsetTracker;
    private RabbitMQRoundRobinAddressSelector addressSelector;
    private boolean isDestroyed = false;


    /**
     * Constructor for RabbitMQListener.
     *
     * @param rabbitmqProperties Properties related to RabbitMQ configuration.
     * @param name               Name of the listener.
     * @param synapseEnvironment Synapse environment for message injection.
     * @param injectingSeq       Sequence to inject messages.
     * @param onErrorSeq         Sequence to handle errors.
     * @param coordination       Whether coordination is enabled.
     * @param sequential         Whether message processing is sequential.
     */
    public RabbitMQListener(Properties rabbitmqProperties, String name, SynapseEnvironment synapseEnvironment,
                            String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {
        super(rabbitmqProperties, name, synapseEnvironment, injectingSeq, onErrorSeq, coordination, sequential);

        // Initialize instance variables
        this.rabbitmqProperties = rabbitmqProperties;
        this.name = name;
        this.synapseEnvironment = synapseEnvironment;
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;

        // Set coordination and sequential properties with defaults if not specified
        this.sequential = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        rabbitmqProperties.getProperty(PollingConstants.INBOUND_ENDPOINT_SEQUENTIAL)
                ),
                true
        );
        this.coordination = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        rabbitmqProperties.getProperty(PollingConstants.INBOUND_COORDINATION)
                ),
                true
        );

        // Initialize the RabbitMQ environment
        initializeRabbitMQEnvironment();
    }


    /**
     * Starts listening to the RabbitMQ queue. Establishes a connection and initializes the consumer
     * based on the queue type specified in the RabbitMQ properties.
     */
    @Override
    public void listen() {
        try {
            // Establish the RabbitMQ connection if not already established
            if (connection == null) {
                if (environment == null) {
                    initializeRabbitMQEnvironment();
                }
                connection = environment.createConnection();
            }
        } catch (RabbitMQException e) {
            log.error("Error while creating the RabbitMQ connection.", e);
            throw new SynapseException("Error while creating the RabbitMQ connection.", e);
        }

        // Initialize the RabbitMQ consumer if not already initialized
        if (rabbitMQConsumer == null) {
            try (Management management = connection.management()) {
                // Retrieve exchange and queue names from properties
                String exchangeName = properties.getProperty(RabbitMQConstants.EXCHANGE_NAME);
                String queueName = properties.getProperty(RabbitMQConstants.QUEUE_NAME);

                // Declare the queue, exchange, and bind them
                RabbitMQUtils.declareQueue(management, queueName, rabbitmqProperties);
                RabbitMQUtils.declareExchange(management, exchangeName, rabbitmqProperties);
                RabbitMQUtils.bindQueueToExchange(management, queueName, exchangeName, rabbitmqProperties);

                // Determine the queue type and initialize the appropriate consumer and message handler
                String queueType = rabbitmqProperties.getProperty(RabbitMQConstants.QUEUE_TYPE);
                Management.QueueType type = Management.QueueType.valueOf(queueType);

                switch (type) {
                    case STREAM:
                        initializeStreamConsumer();
                        break;
                    case CLASSIC:
                        initializeClassicConsumer();
                        break;
                    case QUORUM:
                        initializeQuorumConsumer();
                        break;
                    default:
                        log.warn("Unsupported queue type: " + queueType + ". Default configuration will be used.");
                        initializeClassicConsumer();
                }
            } catch (AmqpException e) {
                log.error("Error while creating the RabbitMQ consumer.", e);
                throw new SynapseArtifactDeploymentException("Error while creating the RabbitMQ consumer.", e);
            }
        }

        log.info("[" + name + "] Starting to listen to the RabbitMQ queue: "
                + rabbitmqProperties.getProperty(RabbitMQConstants.QUEUE_NAME));
    }

    /**
     * Initializes the consumer and message handler for a STREAM queue type.
     */
    private void initializeStreamConsumer() {
        registryOffsetTracker = new RabbitMQRegistryOffsetTracker(
                (AbstractRegistry) synapseEnvironment.getSynapseConfiguration().getRegistry(),
                rabbitmqProperties, name);
        rabbitMqMessageHandler = new StreamQueueMessageHandler(name, injectingSeq, onErrorSeq, sequential,
                synapseEnvironment, rabbitmqProperties, addressSelector, registryOffsetTracker);
        rabbitMQConsumer = RabbitMQUtils.createStreamConsumer(connection, rabbitMqMessageHandler,
                rabbitmqProperties, registryOffsetTracker);
    }

    /**
     * Initializes the consumer and message handler for a CLASSIC queue type.
     */
    private void initializeClassicConsumer() {
        rabbitMqMessageHandler = new ClassicQueueMessageHandler(name, injectingSeq, onErrorSeq, sequential,
                synapseEnvironment, rabbitmqProperties, addressSelector, connection);
        rabbitMQConsumer = RabbitMQUtils.createDefaultConsumer(connection, rabbitMqMessageHandler, rabbitmqProperties);
    }

    /**
     * Initializes the consumer and message handler for a QUORUM queue type.
     */
    private void initializeQuorumConsumer() {
        rabbitMqMessageHandler = new QuorumQueueMessageHandler(name, injectingSeq, onErrorSeq, sequential,
                synapseEnvironment, rabbitmqProperties, addressSelector);
        rabbitMQConsumer = RabbitMQUtils.createDefaultConsumer(connection, rabbitMqMessageHandler, rabbitmqProperties);
    }

    /**
    * Resumes the RabbitMQ listener if it has been destroyed.
    */
   public void resume() {
       if (isDestroyed) {
           // Reinitialize the RabbitMQ environment if it is null
           if (this.environment == null) {
               initializeRabbitMQEnvironment();
           }
           // Start listening to the RabbitMQ queue
           listen();
           isDestroyed = false;
       }
   }

   /**
    * Pauses the RabbitMQ listener by destroying its resources.
    */
   public void pause() {
       destroy();
   }

   /**
    * Initializes the RabbitMQ environment and address selector.
    */
   private void initializeRabbitMQEnvironment() {
       addressSelector = new RabbitMQRoundRobinAddressSelector();
       try {
           environment = new RabbitMQEnvironment(name, rabbitmqProperties, addressSelector);
       } catch (RabbitMQException e) {
           throw new SynapseException("Error occurred while initializing the RabbitMQ environment.", e);
       }
   }

   /**
    * Destroys the RabbitMQ listener by releasing all resources.
    */
   @Override
   public void destroy() {
       // Retrieve the maximum wait time for unsettled messages or use the default value
       long maxWaitTimeMillis = NumberUtils.toLong(
               rabbitmqProperties.getProperty(RabbitMQConstants.MAX_WAIT_TIME_MILLIS),
               RabbitMQConstants.DEFAULT_MAX_WAIT_TIME_MILLIS);

       // Pause the consumer and wait for unsettled messages to clear
       if (rabbitMQConsumer != null) {
           rabbitMQConsumer.pause();
           long startTime = System.currentTimeMillis();

           while (System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
               if (rabbitMQConsumer.unsettledMessageCount() <= 0) {
                   break;
               }
               try {
                   Thread.sleep(100); // Sleep briefly to avoid busy-waiting
               } catch (InterruptedException e) {
                   log.warn("Interrupted while waiting for unsettled messages to clear.", e);
                   Thread.currentThread().interrupt();
                   break;
               }
           }
       }

       // Close and release all RabbitMQ resources
       try {
           if (rabbitMQConsumer != null) {
               rabbitMQConsumer.close();
               rabbitMQConsumer = null;
           }

           if (rabbitMqMessageHandler != null) {
               rabbitMqMessageHandler.shutdown();
               rabbitMqMessageHandler = null;
           }

           if (registryOffsetTracker != null) {
               registryOffsetTracker.shutdown();
           }

           if (connection != null) {
               connection.close();
               connection = null;
           }

           if (environment != null) {
               environment.close();
               environment = null;
               addressSelector = null;
           }
       } catch (Exception e) {
           log.error("Error while closing RabbitMQ resources.", e);
       }

       isDestroyed = true;
   }

}

