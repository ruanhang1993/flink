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

package org.apache.flink.connector.kafka.sink.testutils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connectors.test.common.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connectors.test.common.external.sink.ExternalSystemDataReader;
import org.apache.flink.connectors.test.common.external.sink.TestingSinkSettings;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A Kafka external context that will create only one topic and use partitions in that topic as
 * source splits.
 */
public class KafkaSinkExternalContext implements DataStreamSinkExternalContext<String> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkExternalContext.class);

    private static final String TOPIC_NAME_PREFIX = "kafka-single-topic";
    private static final int DEFAULT_TIMEOUT = 30;
    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    protected String bootstrapServers;
    protected final String topicName;

    private final List<ExternalSystemDataReader<String>> readers = new ArrayList<>();

    protected int numSplits = 0;

    private List<URL> connectorJarPaths;

    protected final AdminClient kafkaAdminClient;

    public KafkaSinkExternalContext(String bootstrapServers, List<URL> connectorJarPaths) {
        this.bootstrapServers = bootstrapServers;
        this.connectorJarPaths = connectorJarPaths;
        this.topicName =
                TOPIC_NAME_PREFIX + "-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        kafkaAdminClient = createAdminClient();
    }

    protected void createTopic(String topicName, int numPartitions, short replicationFactor) {
        LOG.debug(
                "Creating new Kafka topic {} with {} partitions and {} replicas",
                topicName,
                numPartitions,
                replicationFactor);
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            kafkaAdminClient
                    .createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Cannot create topic '%s'", topicName), e);
        }
    }

    protected void deleteTopic(String topicName) {
        LOG.debug("Deleting Kafka topic {}", topicName);
        try {
            kafkaAdminClient
                    .deleteTopics(Collections.singletonList(topicName))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof UnknownTopicOrPartitionException) {
                throw new RuntimeException(String.format("Cannot delete topic '%s'", topicName), e);
            }
        }
    }

    private AdminClient createAdminClient() {
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    @Override
    public Sink<String, ?, ?, ?> createSink(TestingSinkSettings sinkSettings) {
        if (!topicExists(topicName)) {
            createTopic(topicName, 4, (short) 1);
        }

        KafkaSinkBuilder<String> builder = KafkaSink.builder();
        Properties properties = new Properties();
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 900000);
        builder.setBootstrapServers(bootstrapServers)
                .setDeliverGuarantee(sinkSettings.getDeliveryGuarantee())
                .setTransactionalIdPrefix("testingFramework")
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topicName)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build());
        return builder.build();
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        LOG.info("Fetching descriptions for topic: {}", topicName);
        final Map<String, TopicDescription> topicMetadata =
                getTopicMetadata(Arrays.asList(topicName));

        Set<TopicPartition> subscribedPartitions = new HashSet<>();
        for (TopicDescription topic : topicMetadata.values()) {
            for (TopicPartitionInfo partition : topic.partitions()) {
                subscribedPartitions.add(new TopicPartition(topic.name(), partition.partition()));
            }
        }

        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                "flink-kafka-test" + subscribedPartitions.hashCode());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        if (DeliveryGuarantee.EXACTLY_ONCE.equals(sinkSettings.getDeliveryGuarantee())) {
            // default is read_uncommitted
            properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        readers.add(new KafkaDataReader(properties, subscribedPartitions));
        return readers.get(readers.size() - 1);
    }

    @Override
    public List<String> generateTestData(TestingSinkSettings sinkSettings, long seed) {
        Random random = new Random(seed);
        List<String> randomStringRecords = new ArrayList<>();
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        for (int i = 0; i < recordNum; i++) {
            int stringLength = random.nextInt(50) + 1;
            randomStringRecords.add(generateRandomString(stringLength, random));
        }
        return randomStringRecords;
    }

    private String generateRandomString(int length, Random random) {
        String alphaNumericString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            sb.append(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
        }
        return sb.toString();
    }

    protected Map<String, TopicDescription> getTopicMetadata(List<String> topics) {
        try {
            return kafkaAdminClient.describeTopics(topics).all().get();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to get metadata for topics %s.", topics), e);
        }
    }

    private boolean topicExists(String topic) {
        try {
            kafkaAdminClient.describeTopics(Arrays.asList(topic)).all().get();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        if (numSplits != 0) {
            deleteTopic(topicName);
        }
        readers.forEach(
                reader -> {
                    try {
                        reader.close();
                    } catch (Exception e) {
                        kafkaAdminClient.close();
                        throw new RuntimeException("Cannot close split writer", e);
                    }
                });
        readers.clear();
        kafkaAdminClient.close();
    }

    @Override
    public String toString() {
        return "Single-topic Kafka";
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return connectorJarPaths;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
