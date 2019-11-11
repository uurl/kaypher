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
package com.treutec.kaypher.integration;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.KaypherConfigTestUtil;
import com.treutec.kaypher.embedded.KaypherContext;
import com.treutec.kaypher.embedded.KaypherContextTestUtil;
import com.treutec.kaypher.function.TestFunctionRegistry;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.rules.ExternalResource;

/**
 * Junit external resource for managing an instance of {@link KaypherContext}.
 */
public final class TestKaypherContext extends ExternalResource implements AutoCloseable {

  private final IntegrationTestHarness testHarness;
  private final Map<String, Object> additionalConfig;
  private KaypherContext delegate;

  TestKaypherContext(
      final IntegrationTestHarness testHarness,
      final Map<String, Object> additionalConfig
  ) {
    this.testHarness = Objects.requireNonNull(testHarness, "testHarness");
    this.additionalConfig = Objects.requireNonNull(additionalConfig, "additionalConfig");
  }

  public ServiceContext getServiceContext() {
    return delegate.getServiceContext();
  }

  public MetaStore getMetaStore() {
    return delegate.getMetaStore();
  }

  public List<QueryMetadata> sql(final String sql) {
    return delegate.sql(sql);
  }

  List<PersistentQueryMetadata> getPersistentQueries() {
    return delegate.getPersistentQueries();
  }

  void terminateQuery(final QueryId queryId) {
    delegate.terminateQuery(queryId);
  }

  public void ensureStarted() {
    if (delegate != null) {
      return;
    }

    before();
  }

  @Override
  public void close() {
    after();
  }

  @Override
  protected void before() {
    final KaypherConfig kaypherConfig = KaypherConfigTestUtil.create(
        testHarness.kafkaBootstrapServers(),
        additionalConfig
    );

    final SchemaRegistryClient srClient = testHarness
        .getServiceContext()
        .getSchemaRegistryClient();

    delegate = KaypherContextTestUtil
        .create(kaypherConfig, srClient, TestFunctionRegistry.INSTANCE.get());
  }

  @Override
  protected void after() {
    if (delegate != null) {
      delegate.close();
      delegate = null;
    }
  }
}