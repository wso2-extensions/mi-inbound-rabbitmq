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

import com.rabbitmq.client.amqp.Address;
import com.rabbitmq.client.amqp.AddressSelector;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements the `AddressSelector` interface to provide a round-robin
 * strategy for selecting RabbitMQ addresses from a list of available nodes.
 */
public class RabbitMQRoundRobinAddressSelector implements AddressSelector {

    private final AtomicInteger count = new AtomicInteger();
    private Address address;

    /**
     * Selects an address from the provided list of addresses using a round-robin strategy.
     *
     * @param addresses The list of available addresses.
     * @return The selected address.
     * @throws IllegalStateException If the list of addresses is empty.
     */
    @Override
    public Address select(List<Address> addresses) {
        if (addresses.isEmpty()) {
            // Throw an exception if no addresses are available
            throw new IllegalStateException("There should at least one node to connect to");
        } else if (addresses.size() == 1) {
            // If there is only one address, select it directly
            this.address = addresses.get(0);
        } else {
            // Use round-robin selection for multiple addresses
            this.address = addresses.get(count.getAndIncrement() % addresses.size());
        }
        return address;
    }

    /**
     * Retrieves the currently selected address.
     *
     * @return The currently selected address.
     */
    public Address getCurrentAddress() {
        return this.address;
    }
}
