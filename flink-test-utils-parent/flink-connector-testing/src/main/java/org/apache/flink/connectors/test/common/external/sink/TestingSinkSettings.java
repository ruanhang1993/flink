/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.test.common.external.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options for configuring the sink under testing. */
public class TestingSinkSettings {
    private final DeliveryGuarantee deliveryGuarantee;

    public static Builder builder() {
        return new Builder();
    }

    private TestingSinkSettings(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
    }

    /** The delivery guarantee of the source. */
    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public static class Builder {
        private DeliveryGuarantee deliveryGuarantee;

        public Builder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public TestingSinkSettings build() {
            sanityCheck();
            return new TestingSinkSettings(deliveryGuarantee);
        }

        private void sanityCheck() {
            checkNotNull(deliveryGuarantee, "Delivery guarantee is not specified");
        }
    }
}
