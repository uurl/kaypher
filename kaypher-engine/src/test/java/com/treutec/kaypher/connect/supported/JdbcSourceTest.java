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

package com.treutec.kaypher.connect.supported;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.connect.Connector;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.MetaStoreMatchers.OptionalMatchers;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class JdbcSourceTest {

  private final JdbcSource jdbcSource = new JdbcSource();

  @Test
  public void shouldCreateJdbcConnectorWithValidConfigs() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "foo"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    final Connector expected = new Connector(
        "foo",
        foo -> true,
        foo -> foo,
        DataSourceType.KTABLE,
        null);
    assertThat(maybeConnector, OptionalMatchers.of(is(expected)));
  }

  @Test
  public void shouldCreateJdbcConnectorWithValidPrefixTest() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "foo",
        "topic.prefix", "foo-"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    assertThat(
        "expected match",
        maybeConnector.map(connector -> connector.matches("foo-bar")).orElse(false));
  }

  @Test
  public void shouldCreateJdbcConnectorWithValidMapToSource() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "name",
        "topic.prefix", "foo-"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    assertThat(
        maybeConnector.map(connector -> connector.mapToSource("foo-bar")).orElse(null),
        is("name_bar"));
  }

  @Test
  public void shouldCreateJdbcConnectorWithValidConfigsAndSMT() {
    // Given:
    final Map<String, String> config = ImmutableMap.of(
        Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS,
        "name", "foo",
        "transforms", "foobar,createKey",
        "transforms.createKey.type", "org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.createKey.field", "key"
    );

    // When:
    final Optional<Connector> maybeConnector = jdbcSource.fromConfigs(config);

    // Then:
    final Connector expected = new Connector(
        "foo",
        foo -> true,
        foo -> foo,
        DataSourceType.KTABLE,
        "key");
    assertThat(maybeConnector, OptionalMatchers.of(is(expected)));
  }

  @Test
  public void shouldResolveJdbcSourceConfigsTemplate() {
    // Given:
    final Map<String, String> originals = ImmutableMap.<String, String>builder()
        .put(Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS)
        .put("transforms", "foo")
        .put("key", "id")
        .build();

    // When:
    final Map<String, String> resolved = jdbcSource.resolveConfigs(originals);

    // Then:
    assertThat(
        resolved,
        is(ImmutableMap.<String, String>builder()
            .put(Connectors.CONNECTOR_CLASS, JdbcSource.JDBC_SOURCE_CLASS)
            .put("transforms", "foo,kaypherCreateKey,kaypherExtractString")
            .put("transforms.kaypherCreateKey.type", "org.apache.kafka.connect.transforms.ValueToKey")
            .put("transforms.kaypherCreateKey.fields", "id")
            .put("transforms.kaypherExtractString.type", "org.apache.kafka.connect.transforms.ExtractField$Key")
            .put("transforms.kaypherExtractString.field", "id")
            .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
            .put("tasks.max", "1")
            .build()));
  }

}