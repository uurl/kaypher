/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.treutec.kaypher.util;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.logging.processing.NoopProcessingLogContext;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.GenericRowSerDe;
import com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TopicProducer {

  private static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;

  private final Map<String, Object> producerConfig;

  public TopicProducer(final EmbeddedSingleNodeKafkaCluster cluster) {
    this.producerConfig = ImmutableMap.<String, Object>builder()
        .putAll(cluster.getClientProperties())
        .put(ProducerConfig.ACKS_CONFIG, "all")
        .put(ProducerConfig.RETRIES_CONFIG, 0)
        .build();
  }

  /**
   * Topic topicName will be automatically created if it doesn't exist.
   * @param topicName
   * @param recordsToPublish
   * @param schema
   * @return
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws ExecutionException
   */
  public Map<String, RecordMetadata> produceInputData(
      final String topicName,
      final Map<String, GenericRow> recordsToPublish,
      final PhysicalSchema schema
  ) throws InterruptedException, TimeoutException, ExecutionException {

    final Serializer<GenericRow> serializer = GenericRowSerDe.from(
        FormatInfo.of(Format.JSON, Optional.empty(), Optional.empty()),
        schema.valueSchema(),
        new KaypherConfig(ImmutableMap.of()),
        () -> null,
        "ignored",
        NoopProcessingLogContext.INSTANCE
    ).serializer();

    final KafkaProducer<String, GenericRow> producer =
        new KafkaProducer<>(producerConfig, new StringSerializer(), serializer);

    final Map<String, RecordMetadata> result = new HashMap<>();
    for (final Map.Entry<String, GenericRow> recordEntry : recordsToPublish.entrySet()) {
      final String key = recordEntry.getKey();
      final ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(topicName, key, recordEntry.getValue());
      final Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
      result.put(key, recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
  }

  /**
   * Produce input data to the topic named dataProvider.topicName()
   */
  public Map<String, RecordMetadata> produceInputData(final TestDataProvider dataProvider) throws Exception {
    return produceInputData(dataProvider.topicName(), dataProvider.data(), dataProvider.schema());
  }

}
