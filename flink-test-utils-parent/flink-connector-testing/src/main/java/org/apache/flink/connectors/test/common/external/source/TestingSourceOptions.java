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

public class TestingSourceOptions {
    private final Boundedness boundedness;
    private final DeliveryGuarantee deliveryGuarantee;

    public static Builder builder() {
        return new Builder();
    }

    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return this.deliveryGuarantee;
    }

    private TestingSourceOptions(Boundedness boundedness, DeliveryGuarantee deliveryGuarantee) {
        this.boundedness = boundedness;
        this.deliveryGuarantee = deliveryGuarantee;
    }

    public static class Builder {

        private Boundedness boundedness;
        private DeliveryGuarantee deliveryGuarantee;

        public Builder withBoundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        public Builder withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public TestingSourceOptions build() {
            checkNotNull(this.boundedness);
            checkNotNull(this.deliveryGuarantee);
            return new TestingSourceOptions(boundedness, deliveryGuarantee);
        }
    }
}
