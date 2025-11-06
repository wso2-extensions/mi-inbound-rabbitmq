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

import com.rabbitmq.client.amqp.Publisher;

import java.util.concurrent.CompletableFuture;

/**
 * This class implements the RabbitMQ `Publisher.Callback` interface to handle the status of published messages.
 * It uses a `CompletableFuture` to track the result of the message publishing operation.
 */
public class RabbitMQPublisherCallBack implements Publisher.Callback  {

 private final RabbitMQMessageContext rabbitMQMessageContext;

 /**
  * Constructor for RabbitMQPublisherCallBack.
  *
  * @param rabbitMQMessageContext The RabbitMQ message context associated with this callback.
  */
 public RabbitMQPublisherCallBack(RabbitMQMessageContext rabbitMQMessageContext) {
     this.rabbitMQMessageContext = rabbitMQMessageContext;
 }

 // CompletableFuture to track the result of the message publishing operation
 public CompletableFuture<Publisher.Status> result = new CompletableFuture<>();

 /**
  * Handles the status of the published message.
  *
  * @param context The context of the message publishing operation.
  */
 @Override
 public void handle(Publisher.Context context) {
     // Check if the message was accepted
     if (context.status() == Publisher.Status.ACCEPTED) {
         result.complete(Publisher.Status.ACCEPTED);
     } else {
         // Complete exceptionally if the message was not accepted
         result.completeExceptionally(
                 new RabbitMQException("Message with message id: " + rabbitMQMessageContext.getMessageID() +
                         " not accepted: " + context.status(), context.failureCause()));
     }
 }
}
