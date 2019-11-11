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
package com.treutec.kaypher.services;

import com.google.common.collect.Iterables;
import com.treutec.kaypher.util.ExecutorUtil;
import com.treutec.kaypher.util.KaypherServerException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaClusterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterUtil.class);

  private static final long DESCRIBE_CLUSTER_TIMEOUT_SECONDS = 30;

  private KafkaClusterUtil() {

  }

  public static boolean isAuthorizedOperationsSupported(final Admin adminClient) {
    try {
      final DescribeClusterResult authorizedOperations = adminClient.describeCluster(
          new DescribeClusterOptions().includeAuthorizedOperations(true)
      );

      return authorizedOperations.authorizedOperations().get() != null;
    } catch (Exception e) {
      throw new KaypherServerException("Could not get Kafka authorized operations!", e);
    }
  }

  public static Config getConfig(final Admin adminClient) {
    try {
      final Collection<Node> brokers = adminClient.describeCluster().nodes().get();
      final Node broker = Iterables.getFirst(brokers, null);
      if (broker == null) {
        LOG.warn("No available broker found to fetch config info.");
        throw new KaypherServerException(
            "AdminClient discovered an empty Kafka Cluster. "
                + "Check that Kafka is deployed and KAYPHER is properly configured.");
      }

      final ConfigResource configResource = new ConfigResource(
          ConfigResource.Type.BROKER,
          broker.idString()
      );

      final Map<ConfigResource, Config> brokerConfig = ExecutorUtil.executeWithRetries(
          () -> adminClient.describeConfigs(Collections.singleton(configResource)).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);

      return brokerConfig.get(configResource);
    } catch (final KaypherServerException e) {
      throw e;
    } catch (final Exception e) {
      throw new KaypherServerException("Could not get Kafka cluster configuration!", e);
    }
  }

  public static String getKafkaClusterId(final ServiceContext serviceContext) {
    try {
      return serviceContext.getAdminClient()
          .describeCluster()
          .clusterId()
          .get(DESCRIBE_CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to get Kafka cluster information", e);
    }
  }
}
