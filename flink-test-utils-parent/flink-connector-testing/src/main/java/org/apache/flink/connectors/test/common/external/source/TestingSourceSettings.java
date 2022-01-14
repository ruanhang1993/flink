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

package org.apache.flink.connectors.test.common.external.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.DeliveryGuarantee;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options for configuring the source under testing. */
public class TestingSourceSettings {
    private final Boundedness boundedness;
    private final DeliveryGuarantee deliveryGuarantee;

    public static Builder builder() {
        return new Builder();
    }

    /** The boundedness of the source. */
    public Boundedness getBoundedness() {
        return boundedness;
    }

    /** The delivery guarantee of the source. */
    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    private TestingSourceSettings(Boundedness boundedness, DeliveryGuarantee deliveryGuarantee) {
        this.boundedness = boundedness;
        this.deliveryGuarantee = deliveryGuarantee;
    }

    public static class Builder {
        private Boundedness boundedness;
        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE;

        public Builder setBoundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        public Builder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public TestingSourceSettings build() {
            sanityCheck();
            return new TestingSourceSettings(boundedness, deliveryGuarantee);
        }

        private void sanityCheck() {
            checkNotNull(boundedness, "Boundedness is not specified");
            checkNotNull(deliveryGuarantee, "Delivery guarantee is not specified");
        }
    }
}
