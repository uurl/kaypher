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
package com.treutec.kaypher.schema.registry;

import static com.treutec.kaypher.util.ExecutorUtil.RetryBehaviour.ALWAYS;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.util.ExecutorUtil;
import com.treutec.kaypher.util.KaypherConstants;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaRegistryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryUtil.class);

  private static final String CHANGE_LOG_SUFFIX = KaypherConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX
      + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

  private static final String REPARTITION_SUFFIX = KaypherConstants.STREAMS_REPARTITION_TOPIC_SUFFIX
      + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

  private SchemaRegistryUtil() {
  }

  public static void cleanUpInternalTopicAvroSchemas(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    getInternalSubjectNames(applicationId, schemaRegistryClient)
        .forEach(subject -> tryDeleteInternalSubject(applicationId, schemaRegistryClient, subject));
  }

  public static Stream<String> getSubjectNames(final SchemaRegistryClient schemaRegistryClient) {
    return getSubjectNames(
        schemaRegistryClient,
        "Could not get subject names from schema registry.");
  }

  private static Stream<String> getSubjectNames(
      final SchemaRegistryClient schemaRegistryClient, final String errorMsg) {
    try {
      return schemaRegistryClient.getAllSubjects().stream();
    } catch (final Exception e) {
      LOG.warn(errorMsg, e);
      return Stream.empty();
    }
  }

  public static void deleteSubjectWithRetries(
      final SchemaRegistryClient schemaRegistryClient,
      final String subject) throws Exception {
    ExecutorUtil.executeWithRetries(() -> schemaRegistryClient.deleteSubject(subject), ALWAYS);
  }

  private static Stream<String> getInternalSubjectNames(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final Stream<String> allSubjectNames = getSubjectNames(
        schemaRegistryClient,
        "Could not clean up the schema registry for query: " + applicationId);
    return allSubjectNames
        .filter(subjectName -> subjectName.startsWith(applicationId))
        .filter(subjectName ->
            subjectName.endsWith(CHANGE_LOG_SUFFIX) || subjectName.endsWith(REPARTITION_SUFFIX));
  }

  private static void tryDeleteInternalSubject(
      final String applicationId,
      final SchemaRegistryClient schemaRegistryClient,
      final String subjectName
  ) {
    try {
      deleteSubjectWithRetries(schemaRegistryClient, subjectName);
    } catch (final Exception e) {
      LOG.warn("Could not clean up the schema registry for"
          + " query: " + applicationId
          + ", subject: " + subjectName, e);
    }
  }
}