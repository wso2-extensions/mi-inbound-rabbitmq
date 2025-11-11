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

import com.rabbitmq.client.amqp.Message;

import java.util.HashMap;
import java.util.Map;
/**
    * This class holds the RabbitMQ message context information
    */
public class RabbitMQMessageContext {
    private final byte[] body;
    private final String host;
    private final int port;
    private final String queue;
    private final String contentType;
    private final String contentEncoding;
    private final String messageID;
    private final String correlationId;
    private final String replyTo;
    private final boolean hasAnnotations;
    private final Map<String, Object>  annotations = new HashMap<>();
    private final boolean hasProperties;
    private final Map<String, Object>  applicationProperties = new HashMap<>();


    public RabbitMQMessageContext(Message message, String host, int port, String queue) {
        this.body = message.body();
        this.host = host;
        this.port = port;
        this.queue = queue;
        this.contentType = message.contentType();
        this.contentEncoding = message.contentEncoding();
        this.messageID = message.messageIdAsString();
        this.correlationId = message.correlationIdAsString();
        this.replyTo = message.replyTo();
        this.hasAnnotations = message.hasAnnotations();
        if (message.hasAnnotations()) {
            message.forEachAnnotation(annotations::put);
        }
        this.hasProperties = message.hasProperties();
        if (message.hasProperties()) {
            message.forEachProperty(applicationProperties::put);
        }

    }


    public byte[] getBody() {

        return body;
    }

    public String getHost() {

        return host;
    }

    public int getPort() {

        return port;
    }

    public String getQueue() {

        return queue;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public String getMessageID() {
        return messageID;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getReplyTo() {
        return replyTo;
    }
    public Map<String, Object>  getAnnotations() {
        return annotations;
    }

    public Map<String, Object>  getApplicationProperties() {
        return applicationProperties;
    }


    public boolean hasAnnotations() {
        return hasAnnotations;
    }

    public boolean hasProperties() {
        return hasProperties;
    }

}
