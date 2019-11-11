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
package com.treutec.kaypher.security;

import com.treutec.kaypher.services.KafkaClusterUtil;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherServerException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KaypherAuthorizationValidatorFactory {
  private static final Logger LOG = LoggerFactory
      .getLogger(KaypherAuthorizationValidatorFactory.class);
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";
  private static final KaypherAuthorizationValidator DUMMY_VALIDATOR =
      (sc, metastore, statement) -> { };

  private KaypherAuthorizationValidatorFactory() {
  }

  public static KaypherAuthorizationValidator create(
      final KaypherConfig kaypherConfig,
      final ServiceContext serviceContext
  ) {
    final String enabled = kaypherConfig.getString(KaypherConfig.KAYPHER_ENABLE_TOPIC_ACCESS_VALIDATOR);
    if (enabled.equals(KaypherConfig.KAYPHER_ACCESS_VALIDATOR_ON)) {
      LOG.info("Forcing topic access validator");
      return new KaypherAuthorizationValidatorImpl();
    } else if (enabled.equals(KaypherConfig.KAYPHER_ACCESS_VALIDATOR_OFF)) {
      return DUMMY_VALIDATOR;
    }

    final Admin adminClient = serviceContext.getAdminClient();

    if (isKafkaAuthorizerEnabled(adminClient)) {
      if (KafkaClusterUtil.isAuthorizedOperationsSupported(adminClient)) {
        LOG.info("KAYPHER topic authorization checks enabled.");
        return new KaypherAuthorizationValidatorImpl();
      }

      LOG.warn("The Kafka broker has an authorization service enabled, but the Kafka "
          + "version does not support authorizedOperations(). "
          + "KAYPHER topic authorization checks will not be enabled.");
    }
    return DUMMY_VALIDATOR;
  }

  private static boolean isKafkaAuthorizerEnabled(final Admin adminClient) {
    try {
      final ConfigEntry configEntry =
          KafkaClusterUtil.getConfig(adminClient).get(KAFKA_AUTHORIZER_CLASS_NAME);

      return configEntry != null
          && configEntry.value() != null
          && !configEntry.value().isEmpty();
    } catch (final KaypherServerException e) {
      // If ClusterAuthorizationException is thrown is because the AdminClient is not authorized
      // to describe cluster configs. This also means authorization is enabled.
      if (e.getCause() instanceof ClusterAuthorizationException) {
        return true;
      }

      // Throw the unknown exception to avoid leaving the Server unsecured if a different
      // error was thrown
      throw e;
    }
  }
}
