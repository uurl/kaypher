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

import com.treutec.kaypher.exception.KafkaResponseGetFailedException;
import com.treutec.kaypher.util.ExecutorUtil.RetryBehaviour;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

public class KafkaConsumerGroupClientImpl implements KafkaConsumerGroupClient {

  private final Admin adminClient;

  public KafkaConsumerGroupClientImpl(final Admin adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public List<String> listGroups() {
    try {
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.listConsumerGroups().all().get(),
          RetryBehaviour.ON_RETRYABLE)
          .stream()
          .map(ConsumerGroupListing::groupId).collect(Collectors.toList());
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka consumer groups", e);
    }
  }

  public ConsumerGroupSummary describeConsumerGroup(final String group) {

    try {
      final Map<String, ConsumerGroupDescription> groups = ExecutorUtil
          .executeWithRetries(
              () -> adminClient.describeConsumerGroups(Collections.singleton(group)).all().get(),
              RetryBehaviour.ON_RETRYABLE);

      final Set<ConsumerSummary> results = groups
          .values()
          .stream()
          .flatMap(g ->
              g.members()
                  .stream()
                  .map(member -> {
                    final ConsumerSummary summary = new ConsumerSummary(member.consumerId());
                    summary.addPartitions(member.assignment().topicPartitions());
                    return summary;
                  })).collect(Collectors.toSet());

      return new ConsumerGroupSummary(results);

    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to describe Kafka consumer groups", e);
    }
  }
}
