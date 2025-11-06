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
import com.rabbitmq.client.amqp.Publisher;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.Message;
import com.rabbitmq.client.amqp.PublisherBuilder;
import com.rabbitmq.qpid.protonj2.types.Symbol;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.inbound.rabbitmq.message.handler.AbstractRabbitMQMessageHandler;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQRoundRobinAddressSelector;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQAcknowledgementMode;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQConstants;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQMessageContext;
import org.wso2.carbon.inbound.rabbitmq.RabbitMQPublisherCallBack;

import java.time.Duration;

import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles RabbitMQ messages for classic queues.
 * Supports acknowledgment modes: ACCEPTED, REQUEUE, and DISCARDED.
 * Provides dead-letter handling with retry mechanisms and configurable strategies.
 */
public class ClassicQueueMessageHandler extends AbstractRabbitMQMessageHandler {

    private static final Log log = LogFactory.getLog(ClassicQueueMessageHandler.class);
    private final Connection connection;
    private final AtomicInteger activeDeadLetterPublishersInUse = new AtomicInteger(0);
    private ConcurrentHashMap<String, AtomicInteger> deadLetterRetryCountMap;
    private static final Queue<Publisher> deadLetterPublisherPool = new ConcurrentLinkedQueue<>();

/**
         * Constructor for ClassicQueueMessageHandler.
         * Initializes the handler with the provided parameters and sets up the dead letter retry count map
         * if the fixed delay retryable discard strategy is enabled.
         *
         * @param inboundName        The name of the inbound endpoint.
         * @param injectingSeq       The sequence to inject messages into.
         * @param onErrorSeq         The sequence to handle errors.
         * @param sequential         Whether the processing is sequential.
         * @param synapseEnvironment The Synapse environment.
         * @param rabbitMQProperties The RabbitMQ properties.
         * @param addressSelector    The address selector for RabbitMQ.
         * @param connection         The RabbitMQ connection.
         */
        public ClassicQueueMessageHandler(String inboundName, String injectingSeq, String onErrorSeq, boolean sequential,
                                          SynapseEnvironment synapseEnvironment, Properties rabbitMQProperties, RabbitMQRoundRobinAddressSelector addressSelector, Connection connection) {
            // Call the superclass constructor to initialize the base handler
            super(inboundName, injectingSeq, onErrorSeq, sequential, synapseEnvironment, rabbitMQProperties, addressSelector);

            // Assign the RabbitMQ connection
            this.connection = connection;

            // Check if the fixed delay retryable discard strategy is enabled
            if (rabbitMQProperties.containsKey(RabbitMQConstants.CLASSIC_DEAD_LETTER_STRATEGY)
                    && rabbitMQProperties.getProperty(RabbitMQConstants.CLASSIC_DEAD_LETTER_STRATEGY)
                    .equalsIgnoreCase(String.valueOf(RabbitMQConstants.ClassicDeadLetterStrategy.FIXED_DELAY_RETRYABLE_DISCARD))) {
                // Initialize the dead letter retry count map
                deadLetterRetryCountMap = new ConcurrentHashMap<>();
            }
        }



    /**
     * Handles incoming RabbitMQ messages for classic queues.
     * Processes the message and determines the acknowledgment mode.
     *
     * @param consumerContext The RabbitMQ consumer context.
     * @param message         The RabbitMQ message.
     */
    @Override
    public void handle(Consumer.Context consumerContext, Message message) {

        // Retrieve the current RabbitMQ address
        Address address = addressSelector.getCurrentAddress();

        // Initialize the acknowledgment mode
        RabbitMQAcknowledgementMode acknowledgementMode = null;

        // Create a RabbitMQ message context
        RabbitMQMessageContext rabbitMQMsgCtx = new RabbitMQMessageContext(message, address.host(), address.port(), this.queue);

        // Create a Synapse message context
        org.apache.synapse.MessageContext synapseMsgCtx = createMessageContext();
        MessageContext axis2MsgCtx = ((Axis2MessageContext) synapseMsgCtx).getAxis2MessageContext();

        // Process the message and determine the acknowledgment mode
        acknowledgementMode = onMessage(synapseMsgCtx, axis2MsgCtx, rabbitMQMsgCtx, inboundName);

        // Handle message acknowledgment based on the determined mode
        handleAcknowledgement(axis2MsgCtx, rabbitMQMsgCtx, consumerContext, acknowledgementMode);
    }


    /**
       * Handles message acknowledgment for classic queues.
       * Supports acknowledgment modes: ACCEPTED, REQUEUE, and DISCARDED.
       *
       * @param axis2MsgCtx         The Axis2 message context.
       * @param rabbitMQMsgCtx      The RabbitMQ message context.
       * @param context             The RabbitMQ consumer context.
       * @param acknowledgementMode The acknowledgment mode.
       */
      @Override
      protected void handleAcknowledgement(MessageContext axis2MsgCtx, RabbitMQMessageContext rabbitMQMsgCtx, Consumer.Context context, RabbitMQAcknowledgementMode acknowledgementMode) {
          // Retrieve the message ID for logging purposes
          String messageID = rabbitMQMsgCtx.getMessageID() != null ? rabbitMQMsgCtx.getMessageID() : axis2MsgCtx.getMessageID();

          // Handle acknowledgment based on the mode
          switch (Objects.requireNonNull(acknowledgementMode)) {
              case ACCEPTED:
                  // Accept the message
                  context.accept();
                  break;
              case REQUEUE:
                  // Requeue the message
                  handleRequeue(messageID, rabbitMQMsgCtx, context);
                  break;
              case DISCARDED:
                  // Discard the message
                  handleDiscard(messageID, rabbitMQMsgCtx, context);
                  break;
              default:
                  // Log a warning for unknown acknowledgment modes
                  log.warn("[" + inboundName + "] Unknown AcknowledgementMode: " + acknowledgementMode);
                  // Nothing to do here
          }
      }

   /**
          * Handles the requeue operation for a message.
          * If dead letter configurations are present and the override behavior is enabled, the message is discarded.
          * Otherwise, the message is requeued, optionally with a delay if configured.
          *
          * @param messageID       The ID of the message being requeued.
          * @param rabbitMQMsgCtx  The RabbitMQ message context.
          * @param context         The RabbitMQ consumer context.
          */
         private void handleRequeue(String messageID, RabbitMQMessageContext rabbitMQMsgCtx, Consumer.Context context) {
             // Check if dead letter configurations are present
             boolean hasDeadLetterConfig = rabbitMQProperties.containsKey(RabbitMQConstants.DEAD_LETTER_EXCHANGE_NAME) ||
                                           rabbitMQProperties.containsKey(RabbitMQConstants.DEAD_LETTER_QUEUE_NAME) ||
                                           rabbitMQProperties.containsKey(RabbitMQConstants.FINAL_DEAD_LETTER_QUEUE_NAME) ||
                                           rabbitMQProperties.containsKey(RabbitMQConstants.FINAL_DEAD_LETTER_EXCHANGE_NAME);

             // If override behavior is enabled, discard the message instead of requeuing
             if (hasDeadLetterConfig && Boolean.parseBoolean(rabbitMQProperties.getProperty(
                     RabbitMQConstants.OVERRIDE_CLASSIC_QUEUE_MESSAGE_REQUEUE_BEHAVIOR_WITH_DISCARD, "false"))) {
                 handleDiscard(messageID, rabbitMQMsgCtx, context);
             } else {
                 // If a requeue delay is configured, apply the delay
                 if (rabbitMQProperties.containsKey(RabbitMQConstants.MESSAGE_REQUEUE_DELAY)) {
                     String delayValue = rabbitMQProperties.getProperty(RabbitMQConstants.MESSAGE_REQUEUE_DELAY);
                     try {
                         long delay = Long.parseLong(delayValue);
                         Thread.sleep(delay);
                     } catch (NumberFormatException ex) {
                         log.warn("[" + inboundName + "] Invalid requeue delay value '" + delayValue + "' for message id: " + messageID + ". Skipping delay.", ex);
                     } catch (InterruptedException ex) {
                         log.warn("[" + inboundName + "] Thread has been interrupted while delaying message requeue for message id: " + messageID, ex);
                     }
                 }
                 // Requeue the message
                 context.requeue();
                 log.info("[" + inboundName + "] The message with message id: " + messageID + " on the queue: " + this.queue + " will be requeued.");
             }
         }

    /**
 * Handles the discard operation for a message.
 * If the fixed delay retryable discard strategy is not enabled, the message is discarded immediately.
 * Otherwise, it checks for annotations and handles the message based on the dead-lettered count.
 *
 * @param messageID      The ID of the message being discarded.
 * @param rabbitMQMsgCtx The RabbitMQ message context.
 * @param context        The RabbitMQ consumer context.
 */
private void handleDiscard(String messageID, RabbitMQMessageContext rabbitMQMsgCtx, Consumer.Context context) {
    // Check if the fixed delay retryable discard strategy is enabled
    if (!isFixedDelayRetryableDiscardStrategy()) {
        discardMessage(messageID, context);
        return;
    }

    // Check if the message has annotations
    if (rabbitMQMsgCtx.hasAnnotations()) {
        Object xDeathHeaderObj = rabbitMQMsgCtx.getAnnotations().get(RabbitMQConstants.X_OPT_DEATHS);

        // Extract the x-death header as a list of maps
        List<Map<Symbol, Object>> xDeathHeader = xDeathHeaderObj instanceof Map[]
                ? Arrays.asList((Map<Symbol, Object>[]) xDeathHeaderObj)
                : Collections.emptyList();

        // Retrieve the maximum dead-lettered count from properties
        int maxDeadLetteredCount;
        String maxDeadLetteredCountStr = rabbitMQProperties.getProperty(RabbitMQConstants.MAX_DEAD_LETTERED_COUNT);
        if (maxDeadLetteredCountStr != null && !maxDeadLetteredCountStr.trim().isEmpty()) {
            try {
                maxDeadLetteredCount = Integer.parseInt(maxDeadLetteredCountStr.trim());
            } catch (NumberFormatException e) {
                log.warn("[" + inboundName + "] Invalid value for "+ RabbitMQConstants.MAX_DEAD_LETTERED_COUNT + " : " + maxDeadLetteredCountStr + ". Using default value -1.");
                maxDeadLetteredCount = -1;
            }
        } else {
            maxDeadLetteredCount = -1;
        }

        // Handle the message if x-death header is present and max dead-lettered count is valid
        if (!xDeathHeader.isEmpty() && maxDeadLetteredCount != -1) {
            handleDeadLetteredMessage(messageID, xDeathHeader, maxDeadLetteredCount, rabbitMQMsgCtx, context);
        } else {
            // Discard the message if no valid x-death header or max count is invalid
            discardMessage(messageID, context);
        }
    } else {
        // Discard the message if no annotations are present
        discardMessage(messageID, context);
    }
}

    /**
     * Checks if the fixed delay retryable discard strategy is enabled.
     *
     * @return true if the strategy is enabled, false otherwise.
     */
    private boolean isFixedDelayRetryableDiscardStrategy() {
        // Check if the RabbitMQ properties contain the classic dead letter strategy
        return rabbitMQProperties.containsKey(RabbitMQConstants.CLASSIC_DEAD_LETTER_STRATEGY) &&
                rabbitMQProperties.getProperty(RabbitMQConstants.CLASSIC_DEAD_LETTER_STRATEGY)
                        .equalsIgnoreCase(String.valueOf(RabbitMQConstants.ClassicDeadLetterStrategy.FIXED_DELAY_RETRYABLE_DISCARD));
    }

    /**
     * Handles a dead-lettered message based on the dead-letter count.
     * If the count is within the maximum allowed, the message is discarded.
     * Otherwise, it proceeds to handle the message after exceeding the max count.
     *
     * @param messageID           The ID of the message.
     * @param xDeathHeader        The x-death header containing dead-letter information.
     * @param maxDeadLetteredCount The maximum allowed dead-letter count.
     * @param rabbitMQMsgCtx      The RabbitMQ message context.
     * @param context             The RabbitMQ consumer context.
     */
    private void handleDeadLetteredMessage(String messageID, List<Map<Symbol, Object>> xDeathHeader, int maxDeadLetteredCount,
                                           RabbitMQMessageContext rabbitMQMsgCtx, Consumer.Context context) {
        // Retrieve the dead-letter count from the x-death header
        Long count = getDeadLetterCount(xDeathHeader);

        // If the count is within the maximum allowed, discard the message
        if (count != null && count <= maxDeadLetteredCount) {
            context.discard();
            log.info("[" + inboundName + "] The rejected message with message id: " + messageID +
                    " on the queue: " + this.queue + " is dead-lettered " + count + " time(s).");
        } else if (count == null ) {
            context.discard();
            log.info("[" + inboundName + "] The rejected message with message id: " + messageID +
                    " on the queue: " + this.queue + " is dead-lettered ");
        } else {
            // Handle the message after exceeding the max dead-letter count
            proceedAfterMaxDeadLetteredCount(messageID, rabbitMQMsgCtx, context);
        }
    }

    /**
     * Retrieves the dead-letter count for the current queue from the x-death header.
     *
     * @param xDeathHeader The x-death header containing dead-letter information.
     * @return The dead-letter count for the current queue, or null if not found.
     */
    private Long getDeadLetterCount(List<Map<Symbol, Object>> xDeathHeader) {
        // Iterate through the x-death header entries
        for (Map<Symbol, Object> deathEntry : xDeathHeader) {
            // Check if the queue matches the current queue
            if (this.queue.equals(deathEntry.get(Symbol.getSymbol("queue")))) {
                // Return the dead-letter count for the current queue
                Object countObj = deathEntry.get(Symbol.getSymbol("count"));
                if (countObj != null) {
                    try {
                        return Long.parseLong(countObj.toString());
                    } catch (NumberFormatException e) {
                        log.warn("[" + inboundName + "] Unable to parse dead-letter count from x-death header for queue: " + this.queue + ". Value: " + countObj, e);
                        return null;
                    }
                }
                return null;
            }
        }
        // Return null if no matching entry is found
        return null;
    }

    /**
     * Discards a message and logs the operation.
     *
     * @param messageID The ID of the message being discarded.
     * @param context   The RabbitMQ consumer context.
     */
    private void discardMessage(String messageID, Consumer.Context context) {
        // Discard the message
        context.discard();
        log.info("[" + inboundName + "] The rejected message with message id: " + messageID +
                " on the queue: " + this.queue + " will discard or dead-lettered.");
    }

   /**
         * Handles the scenario when the maximum dead-lettered count is exceeded.
         * Attempts to publish the message to the final dead letter queue or exchange.
         *
         * @param messageID       The ID of the message.
         * @param messageContext  The RabbitMQ message context.
         * @param consumerContext The RabbitMQ consumer context.
         */
        private void proceedAfterMaxDeadLetteredCount(String messageID, RabbitMQMessageContext messageContext, Consumer.Context consumerContext) {
            // Retrieve a dead letter publisher from the pool or create a new one
            Publisher deadLetterPublisher = getDeadLetterPublisher(connection,
                    rabbitMQProperties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_QUEUE_NAME),
                    rabbitMQProperties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_EXCHANGE_NAME),
                    rabbitMQProperties.getProperty(RabbitMQConstants.FINAL_DEAD_LETTER_ROUTING_KEY),
                    Duration.ofMillis(rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_ACK_WAIT_TIME) != null ?
                            Long.parseLong(rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_ACK_WAIT_TIME)) :
                            (long) RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_ACK_WAIT_TIME));

            // If no valid dead letter publisher is available, log and accept the message
            if (deadLetterPublisher == null) {
                log.info("[" + inboundName + "] The max dead lettered count exceeded. No valid configuration for publishing the message. " +
                        "Message with message id: " + messageContext.getMessageID() + " will discard.");
                consumerContext.accept();
                return;
            }

            // Publish the message to the dead letter queue
            publishToDeadLetterQueue(messageID, messageContext, consumerContext, deadLetterPublisher);
        }

        /**
         * Publishes a message to the dead letter queue or exchange.
         * Handles retries and errors during the publishing process.
         *
         * @param messageID          The ID of the message.
         * @param messageContext     The RabbitMQ message context.
         * @param consumerContext    The RabbitMQ consumer context.
         * @param deadLetterPublisher The publisher for the dead letter queue or exchange.
         */
        private void publishToDeadLetterQueue(String messageID, RabbitMQMessageContext messageContext, Consumer.Context consumerContext, Publisher deadLetterPublisher) {
            // Clone the message with the current context and assign a message ID
            Message message = cloneMessageWithContext(messageContext, deadLetterPublisher.message(messageContext.getBody()).messageId(messageID), false);
            RabbitMQPublisherCallBack publisherCallBack = new RabbitMQPublisherCallBack(messageContext);

            // Publish the message using the dead letter publisher
            deadLetterPublisher.publish(message, publisherCallBack);

            try {

                // Retrieve the retry interval from properties or use the default value
                long retryInterval;
                String retryIntervalStr = rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_INTERVAL);
                if (retryIntervalStr != null) {
                    try {
                        retryInterval = Long.parseLong(retryIntervalStr);
                    } catch (NumberFormatException e) {
                        log.warn("[" + inboundName + "] Invalid value for " + RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_INTERVAL +  " : '" + retryIntervalStr + "'. Using default: " + RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT, e);
                        retryInterval = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_INTERVAL;
                    }
                } else {
                    retryInterval = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_INTERVAL;
                }

                // Handle the publishing result based on the callback status
                handlePublishingResult(messageID, messageContext, consumerContext, deadLetterPublisher, publisherCallBack.result.get(retryInterval, TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                // Log and handle timeout exceptions during publishing
                log.error("[" + inboundName + "] Publish to Final Dead Letter operation timed out for message id: " + messageID, e);
                handleDeadLetterPublishingFailures(deadLetterPublisher.message(messageContext.getBody()).messageId(messageID), messageContext, deadLetterPublisher);
                consumerContext.accept();
            } catch (ExecutionException | InterruptedException e) {
                // Log and handle execution or interruption exceptions during publishing
                log.error("[" + inboundName + "] Publish to Final Dead Letter operation encountered an error for message id: " + messageID, e);
                handleDeadLetterPublishingFailures(deadLetterPublisher.message(messageContext.getBody()).messageId(messageID), messageContext, deadLetterPublisher);
                consumerContext.accept();
            } finally {
                // Release the dead letter publisher back to the pool
                releaseDeadLetterPublisher(deadLetterPublisher);
            }
        }

    /**
        * Handles the result of publishing a message to the dead letter queue or exchange.
        * If the publishing is successful, the message is accepted. Otherwise, it retries or logs the failure.
        *
        * @param messageID          The ID of the message being published.
        * @param messageContext     The RabbitMQ message context.
        * @param consumerContext    The RabbitMQ consumer context.
        * @param deadLetterPublisher The publisher for the dead letter queue or exchange.
        * @param status             The status of the publishing operation.
        */
       private void handlePublishingResult(String messageID, RabbitMQMessageContext messageContext, Consumer.Context consumerContext, Publisher deadLetterPublisher, Publisher.Status status) {
           if (status == Publisher.Status.ACCEPTED) {
               // Log success and accept the message
               log.info("[" + inboundName + "] Message with message id: " + messageID + " successfully published to the dead letter queue/exchange after exceeding max dead lettered count.");
               consumerContext.accept();
           } else {
               // Handle publishing failures and log the error
               handleDeadLetterPublishingFailures(deadLetterPublisher.message(messageContext.getBody()).messageId(messageID), messageContext, deadLetterPublisher);
               log.error("[" + inboundName + "] Message with message id: " + messageID + " failed to publish after exceeding max dead lettered count.");
               consumerContext.accept();
           }
       }

       /**
        * Handles failures during the publishing of a message to the dead letter queue.
        * Submits a retry task to the executor service.
        *
        * @param message            The message that failed to publish.
        * @param messageContext     The RabbitMQ message context.
        * @param deadLetterPublisher The publisher for the dead letter queue or exchange.
        */
       private void handleDeadLetterPublishingFailures(Message message, RabbitMQMessageContext messageContext, Publisher deadLetterPublisher) {
           // Generate a unique message ID if not already present
           String messageId = Optional.ofNullable(message.messageIdAsString()).orElse(UUID.randomUUID().toString());
           deadLetterRetryCountMap.putIfAbsent(messageId, new AtomicInteger(1));
           AtomicInteger retryAttempts = deadLetterRetryCountMap.get(messageId);

           // Retrieve a thread from the Synapse thread pool to submit the retry task
           ExecutorService executor = synapseEnvironment.getExecutorService();
           executor.submit(() -> retryPublishing(messageId, message, messageContext, deadLetterPublisher, retryAttempts));
       }

       /**
        * Retries publishing a message to the dead letter queue or exchange.
        * Applies exponential backoff for retry intervals and stops after reaching the maximum retry attempts.
        *
        * @param messageId          The ID of the message being retried.
        * @param message            The message to be published.
        * @param messageContext     The RabbitMQ message context.
        * @param deadLetterPublisher The publisher for the dead letter queue or exchange.
        * @param retryAttempts      The current retry attempt count.
        */
       private void retryPublishing(String messageId, Message message, RabbitMQMessageContext messageContext, Publisher deadLetterPublisher, AtomicInteger retryAttempts) {
           // Retrieve the maximum retry attempts from properties or use the default value
           int maxRetryAttempt;
           String maxRetryAttemptStr = rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_COUNT);
           if (maxRetryAttemptStr != null) {
               try {
                   maxRetryAttempt = Integer.parseInt(maxRetryAttemptStr);
               } catch (NumberFormatException e) {
                   log.warn("[" + inboundName + "] Invalid value for "+ RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_COUNT+ " : '" + maxRetryAttemptStr + "'. Using default: " + RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT, e);
                   maxRetryAttempt = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT;
               }
           } else {
               maxRetryAttempt = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT;
           }

           // Retry publishing until the maximum retry attempts are reached
           while (retryAttempts.get() < maxRetryAttempt) {
               try {
                   // Create a callback for the publishing operation
                   RabbitMQPublisherCallBack publisherCallBack = new RabbitMQPublisherCallBack(messageContext);
                   deadLetterPublisher.publish(message, publisherCallBack);

                   // Retrieve retry interval and exponential factor from properties or use default values

                   long retryInterval;
                   String retryIntervalStr = rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_INTERVAL);
                   if (retryIntervalStr != null) {
                       try {
                           retryInterval = Long.parseLong(retryIntervalStr);
                       } catch (NumberFormatException e) {
                           log.warn("[" + inboundName + "] Invalid value for "+ RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_INTERVAL+ " : '" + maxRetryAttemptStr + "'. Using default: " + RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT, e);
                           retryInterval = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_INTERVAL;
                       }
                   } else {
                       retryInterval = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_INTERVAL;
                   }

                   float exponentialFactor;
                   String exponentialFactorStr = rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_EXPONENTIAL_FACTOR);
                   if (exponentialFactorStr != null) {
                       try {
                           exponentialFactor = Float.parseFloat(exponentialFactorStr);
                       } catch (NumberFormatException e) {
                           log.warn("[" + inboundName + "] Invalid value for "+ RabbitMQConstants.DEAD_LETTER_PUBLISHER_RETRY_EXPONENTIAL_FACTOR+ " : '" + maxRetryAttemptStr + "'. Using default: " + RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_COUNT, e);
                           exponentialFactor = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_EXPONENTIAL_FACTOR;
                       }
                   } else {
                       exponentialFactor = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_RETRY_EXPONENTIAL_FACTOR;
                   }


                   // Calculate the timeout for the current retry attempt
                   long timeout = (long) (retryInterval * Math.pow(exponentialFactor, retryAttempts.get()));

                   // Check the result of the publishing operation
                   if (publisherCallBack.result.get(timeout, TimeUnit.MILLISECONDS) == Publisher.Status.ACCEPTED) {
                       // Log success and remove the message from the retry map
                       log.info("[" + inboundName + "] Message with message id: " + messageId + " successfully published after " + retryAttempts.get() + " attempt(s).");
                       deadLetterRetryCountMap.remove(messageId);
                       return;
                   }
               } catch (TimeoutException | ExecutionException | InterruptedException e) {
                   // Log the failure and increment the retry attempt count
                   log.warn("[" + inboundName + "] Retry attempt " + retryAttempts.incrementAndGet() + " failed for message id: " + messageId, e);
               }
           }

           // Log that the maximum retry attempts have been reached and discard the message
           log.info("[" + inboundName + "] Message with message id: " + messageId + " reached max publishing attempts. Discarding the message.");
       }


    /**
           * Retrieves a dead letter publisher from the pool or creates a new one if none are available.
           * Configures the publisher with the provided queue, exchange, and routing key details.
           *
           * @param connection   The RabbitMQ connection.
           * @param queueName    The name of the dead letter queue.
           * @param exchangeName The name of the dead letter exchange.
           * @param routingKey   The routing key for the dead letter exchange.
           * @param timeout      The timeout duration for the publisher.
           * @return A configured Publisher instance or null if insufficient information is provided.
           */
          private Publisher getDeadLetterPublisher(Connection connection, String queueName, String exchangeName, String routingKey, Duration timeout) {
              // Attempt to retrieve a publisher from the pool
              Publisher publisher = deadLetterPublisherPool.poll();
              if (publisher == null) {
                  // Create a new publisher builder with the specified timeout
                  PublisherBuilder builder = connection.publisherBuilder().publishTimeout(timeout);

                  // Configure the publisher with the queue, exchange, and routing key if provided
                  if (StringUtils.isNotEmpty(queueName)) {
                      builder.queue(queueName);
                  }
                  if (StringUtils.isNotEmpty(exchangeName)) {
                      builder.exchange(exchangeName);
                  }
                  if (StringUtils.isNotEmpty(routingKey)) {
                      builder.key(routingKey);
                  }

                  // Build the publisher if sufficient information is provided
                  if (StringUtils.isNotEmpty(queueName) || StringUtils.isNotEmpty(exchangeName)) {
                      publisher = builder.build();
                  } else {
                      // Log a warning if insufficient information is provided
                      log.warn("[" + inboundName + "] Insufficient information provided to create Dead Letter Publisher. Dead Letter Publisher will not be created and messages won't publish to final dead letter exchange once the maximum dead lettered count exceeded.");
                      return null;
                  }
              }
              // Increment the count of active publishers in use
              activeDeadLetterPublishersInUse.incrementAndGet();
              return publisher;
          }

          /**
           * Releases a dead letter publisher back to the pool.
           * Decrements the count of active publishers in use.
           *
           * @param publisher The Publisher instance to be released.
           */
          private void releaseDeadLetterPublisher(Publisher publisher) {
              if (publisher != null) {
                  // Add the publisher back to the pool
                  boolean offered = deadLetterPublisherPool.offer(publisher);
                  if (!offered) {
                      log.warn("[" + inboundName + "] Failed to return Publisher to DeadLetterPublisherPool.");
                  }
                  // Decrement the count of active publishers in use
                  activeDeadLetterPublishersInUse.decrementAndGet();
              }
          }

          /**
           * Shuts down the ClassicQueueMessageHandler.
           * Clears the dead letter publisher pool after ensuring no publishers are in use.
           */
          @Override
          public void shutdown() {
              // Retrieve the shutdown timeout from properties or use the default value
              long shutdownTimeout;
              String shutdownTimeoutStr = rabbitMQProperties.getProperty(RabbitMQConstants.DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT);
              if (shutdownTimeoutStr != null) {
                  try {
                      shutdownTimeout = Long.parseLong(shutdownTimeoutStr);
                  } catch (NumberFormatException e) {
                      log.warn("[" + inboundName + "] Invalid value for " + RabbitMQConstants.DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT + ": ' " + shutdownTimeoutStr + "'. Using default: " + RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT, e);
                      shutdownTimeout = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT;
                  }
              } else {
                  shutdownTimeout = RabbitMQConstants.DEFAULT_DEAD_LETTER_PUBLISHER_SHUTDOWN_TIMEOUT;
              }

              // Create a scheduled executor service to periodically check the pool status
              ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
              long startTime = System.currentTimeMillis();

              // Schedule a task to clear the publisher pool at fixed intervals
              long finalShutdownTimeout = shutdownTimeout;
              scheduler.scheduleAtFixedRate(() -> {
                  try {
                      // Check if the retry count map is empty or the shutdown timeout has been reached
                      if (deadLetterRetryCountMap.isEmpty() || (System.currentTimeMillis() - startTime) >= finalShutdownTimeout) {
                          // Clear the pool if no publishers are in use
                          if (activeDeadLetterPublishersInUse.get() == 0) {
                              deadLetterPublisherPool.clear();
                              log.info("[" + inboundName + "] DeadLetterPublisherPool cleared successfully.");
                              // Shut down the scheduler if not already shut down
                              if (!scheduler.isShutdown()) {
                                  scheduler.shutdown();
                              }
                          } else {
                              // Log a warning if publishers are still in use
                              log.warn("[" + inboundName + "] DeadLetterPublisherPool still has publishers in use. Retrying...");
                          }
                      }
                  } catch (Exception e) {
                      // Log any errors that occur during the clearing process
                      log.error("[" + inboundName + "] An error occurred while clearing the DeadLetterPublisherPool.", e);
                  }
              }, 0, 5, TimeUnit.SECONDS);
          }


}
