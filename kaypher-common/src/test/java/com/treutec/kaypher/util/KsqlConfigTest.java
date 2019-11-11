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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.errors.LogMetricAndContinueExceptionHandler;
import com.treutec.kaypher.errors.ProductionExceptionHandlerUtil.LogAndContinueProductionExceptionHandler;
import com.treutec.kaypher.errors.ProductionExceptionHandlerUtil.LogAndFailProductionExceptionHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KaypherConfigTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldSetInitialValuesCorrectly() {
    final Map<String, Object> initialProps = new HashMap<>();
    initialProps.put(KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 10);
    initialProps.put(KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);
    initialProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 800);
    initialProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

    final KaypherConfig kaypherConfig = new KaypherConfig(initialProps);

    assertThat(kaypherConfig.getInt(KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY), equalTo(10));
    assertThat(kaypherConfig.getShort(KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY), equalTo((short) 3));
  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerByDefault() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.emptyMap());
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogMetricAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerWhenFailOnDeserializationErrorFalse() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(KaypherConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, false));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogMetricAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldNotSetDeserializationExceptionHandlerWhenFailOnDeserializationErrorTrue() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(KaypherConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, true));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, nullValue());
  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerWhenFailOnProductionErrorFalse() {
    final KaypherConfig kaypherConfig =
        new KaypherConfig(Collections.singletonMap(KaypherConfig.FAIL_ON_PRODUCTION_ERROR_CONFIG, false));
    final Object result = kaypherConfig.getKaypherStreamConfigProps()
        .get(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogAndContinueProductionExceptionHandler.class));
  }

  @Test
  public void shouldNotSetDeserializationExceptionHandlerWhenFailOnProductionErrorTrue() {
    final KaypherConfig kaypherConfig =
        new KaypherConfig(Collections.singletonMap(KaypherConfig.FAIL_ON_PRODUCTION_ERROR_CONFIG, true));
    final Object result = kaypherConfig.getKaypherStreamConfigProps()
        .get(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogAndFailProductionExceptionHandler.class));
  }

  @Test
  public void shouldFailOnProductionErrorByDefault() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.emptyMap());
    final Object result = kaypherConfig.getKaypherStreamConfigProps()
        .get(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogAndFailProductionExceptionHandler.class));
  }

  @Test
  public void shouldSetStreamsConfigConsumerUnprefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("earliest"));
  }

  @Test
  public void shouldSetStreamsConfigConsumerPrefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(
            StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
        .get(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        equalTo(100));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigConsumerKaypherPrefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(
            KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        equalTo(100));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(nullValue()));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigProducerUnprefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024"));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(ProducerConfig.BUFFER_MEMORY_CONFIG);
    assertThat(result, equalTo(1024L));
  }

  @Test
  public void shouldSetStreamsConfigProducerPrefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(
            StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "1024"));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
        .get(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        equalTo(1024L));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigTopicUnprefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 2));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    assertThat(result, equalTo(2));
  }

  @Test
  public void shouldSetStreamsConfigKaypherTopicPrefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(
            KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.TOPIC_PREFIX + TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 2));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(StreamsConfig.TOPIC_PREFIX + TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
        equalTo(2));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigKaypherProducerPrefixedProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(
            KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "1024"));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        equalTo(1024L));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(nullValue()));

    assertThat(kaypherConfig.getKaypherStreamConfigProps()
            .get(KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigAdminClientProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, 3));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(
        AdminClientConfig.RETRIES_CONFIG);
    assertThat(result, equalTo(3));
  }

  @Test
  public void shouldSetStreamsConfigProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(
        Collections.singletonMap(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "128"));
    final Object result = kaypherConfig.getKaypherStreamConfigProps().get(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG);
    assertThat(result, equalTo(128L));
  }

  @Test
  public void shouldSetPrefixedStreamsConfigProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(
        KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "128"));

    assertThat(kaypherConfig.getKaypherStreamConfigProps().
        get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG), equalTo(128L));

    assertThat(kaypherConfig.getKaypherStreamConfigProps().
        get(KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetMonitoringInterceptorConfigProperties() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(
        "confluent.monitoring.interceptor.topic", "foo"));
    final Object result
        = kaypherConfig.getKaypherStreamConfigProps().get("confluent.monitoring.interceptor.topic");
    assertThat(result, equalTo("foo"));
  }

  @Test
  public void shouldSetMonitoringInterceptorConfigPropertiesByClientType() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        "kaypher.streams.consumer.confluent.monitoring.interceptor.topic", "foo",
        "producer.confluent.monitoring.interceptor.topic", "bar"
    );

    final KaypherConfig kaypherConfig = new KaypherConfig(props);

    // When:
    final Map<String, Object> result = kaypherConfig.getKaypherStreamConfigProps();

    // Then:
    assertThat(result.get("consumer.confluent.monitoring.interceptor.topic"), is("foo"));
    assertThat(result.get("producer.confluent.monitoring.interceptor.topic"), is("bar"));
  }

  @Test
  public void shouldFilterPropertiesForWhichTypeUnknown() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap("you.shall.not.pass", "wizard"));
    assertThat(
        kaypherConfig.getAllConfigPropsWithSecretsObfuscated().keySet(),
        not(hasItem("you.shall.not.pass")));
  }

  @Test
  public void shouldCloneWithKaypherPropertyOverwrite() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(
        KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "test"));
    final KaypherConfig kaypherConfigClone = kaypherConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "test-2"));
    final String result = kaypherConfigClone.getString(KaypherConfig.KAYPHER_SERVICE_ID_CONFIG);
    assertThat(result, equalTo("test-2"));
  }

  @Test
  public void shouldCloneWithStreamPropertyOverwrite() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));
    final KaypherConfig kaypherConfigClone = kaypherConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "200"));
    final Object result = kaypherConfigClone.getKaypherStreamConfigProps().get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
    assertThat(result, equalTo(200));
  }

  @Test
  public void shouldHaveCorrectOriginalsAfterCloneWithOverwrite() {
    // Given:
    final KaypherConfig initial = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "original-id",
        KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS, "on"
    ));

    // When:
    final KaypherConfig cloned = initial.cloneWithPropertyOverwrite(ImmutableMap.of(
        KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "overridden-id",
        KaypherConfig.KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "bob"
    ));

    // Then:
    assertThat(cloned.originals(), is(ImmutableMap.of(
        KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "overridden-id",
        KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS, "on",
        KaypherConfig.KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "bob"
    )));
  }

  @Test
  public void shouldCloneWithUdfProperty() {
    // Given:
    final String functionName = "bob";
    final String settingPrefix = KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + functionName + ".";

    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        settingPrefix + "one", "should-be-cloned",
        settingPrefix + "two", "should-be-overwritten"
    ));

    // When:
    final KaypherConfig cloned = config.cloneWithPropertyOverwrite(ImmutableMap.of(
        settingPrefix + "two", "should-be-new-value"
    ));

    // Then:
    assertThat(cloned.getKaypherFunctionsConfigProps(functionName), is(ImmutableMap.of(
        settingPrefix + "one", "should-be-cloned",
        settingPrefix + "two", "should-be-new-value"
    )));
  }

  @Test
  public void shouldCloneWithMultipleOverwrites() {
    final KaypherConfig kaypherConfig = new KaypherConfig(ImmutableMap.of(
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "123",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
    ));
    final KaypherConfig clone = kaypherConfig.cloneWithPropertyOverwrite(ImmutableMap.of(
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2",
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "456"
    ));
    final KaypherConfig cloneClone = clone.cloneWithPropertyOverwrite(ImmutableMap.of(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        StreamsConfig.METADATA_MAX_AGE_CONFIG, "13"
    ));
    final Map<String, ?> props = cloneClone.getKaypherStreamConfigProps();
    assertThat(props.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG), equalTo(456));
    assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), equalTo("earliest"));
    assertThat(props.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG), equalTo(2));
    assertThat(props.get(StreamsConfig.METADATA_MAX_AGE_CONFIG), equalTo(13L));
  }

  @Test
  public void shouldCloneWithPrefixedStreamPropertyOverwrite() {
    final KaypherConfig kaypherConfig = new KaypherConfig(Collections.singletonMap(
        KaypherConfig.KAYPHER_STREAMS_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));
    final KaypherConfig kaypherConfigClone = kaypherConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            KaypherConfig.KAYPHER_STREAMS_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "200"));
    final Object result = kaypherConfigClone.getKaypherStreamConfigProps().get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
    assertThat(result, equalTo(200));
  }

  @Test
  public void shouldPreserveOriginalCompatibilitySensitiveConfigs() {
    final Map<String, String> originalProperties = ImmutableMap.of(
        KaypherConfig.KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "not_the_default");
    final KaypherConfig currentConfig = new KaypherConfig(Collections.emptyMap());
    final KaypherConfig compatibleConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(originalProperties);
    assertThat(
        compatibleConfig.getString(KaypherConfig.KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG),
        equalTo("not_the_default"));
  }

  @Test
  public void shouldUseCurrentValueForCompatibilityInsensitiveConfigs() {
    final Map<String, String> originalProperties = Collections.singletonMap(KaypherConfig.KAYPHER_ENABLE_UDFS, "false");
    final KaypherConfig currentConfig = new KaypherConfig(Collections.singletonMap(KaypherConfig.KAYPHER_ENABLE_UDFS, true));
    final KaypherConfig compatibleConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(originalProperties);
    assertThat(compatibleConfig.getBoolean(KaypherConfig.KAYPHER_ENABLE_UDFS), is(true));
  }

  @Test
  public void shouldReturnUdfConfig() {
    // Given:
    final String functionName = "bob";

    final String udfConfigName =
        KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + functionName + ".some-setting";

    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        udfConfigName, "should-be-visible"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKaypherFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.get(udfConfigName), is("should-be-visible"));
  }

  @Test
  public void shouldReturnUdfConfigOnlyIfLowercase() {
    // Given:
    final String functionName = "BOB";

    final String correctConfigName =
        KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + functionName.toLowerCase() + ".some-setting";

    final String invalidConfigName =
        KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + functionName + ".some-other-setting";

    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        invalidConfigName, "should-not-be-visible",
        correctConfigName, "should-be-visible"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKaypherFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.keySet(), contains(correctConfigName));
  }

  @Test
  public void shouldReturnUdfConfigAfterMerge() {
    final String functionName = "BOB";

    final String correctConfigName =
        KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + functionName.toLowerCase() + ".some-setting";

    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        correctConfigName, "should-be-visible"
    ));
    final KaypherConfig merged = config.overrideBreakingConfigsWithOriginalValues(Collections.emptyMap());

    // When:
    final Map<String, ?> udfProps = merged.getKaypherFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.keySet(), hasItem(correctConfigName));
  }

  @Test
  public void shouldReturnGlobalUdfConfig() {
    // Given:
    final String globalConfigName =
        KaypherConfig.KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX + ".some-setting";

    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        globalConfigName, "global"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKaypherFunctionsConfigProps("what-eva");

    // Then:
    assertThat(udfProps.get(globalConfigName), is("global"));
  }

  @Test
  public void shouldNotReturnNoneUdfConfig() {
    // Given:
    final String functionName = "bob";
    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "not a udf property",
        KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + "different_udf.some-setting", "different udf property"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKaypherFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.keySet(), is(empty()));
  }

  @Test
  public void shouldListKnownKaypherConfig() {
    // Given:
    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_SERVICE_ID_CONFIG, "not sensitive",
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "sensitive!"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KaypherConfig.KAYPHER_SERVICE_ID_CONFIG), is("not sensitive"));
  }

  @Test
  public void shouldListKnownKaypherFunctionConfig() {
    // Given:
    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG, "true"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KaypherConfig.KAYPHER_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG), is("true"));
  }

  @Test
  public void shouldListUnknownKaypherFunctionConfigObfuscated() {
    // Given:
    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop", "maybe sensitive"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KaypherConfig.KAYPHER_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop"),
        is("[hidden]"));
  }

  @Test
  public void shouldListKnownStreamsConfigObfuscated() {
    // Given:
    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "not sensitive",
        KaypherConfig.KAYPHER_STREAMS_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "sensitive!",
        KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX +
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "sensitive!"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.APPLICATION_ID_CONFIG),
        is("not sensitive"));
    assertThat(result.get(
        KaypherConfig.KAYPHER_STREAMS_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
        is("[hidden]"));
    assertThat(result.get(KaypherConfig.KAYPHER_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
        is("[hidden]"));
  }

  @Test
  public void shouldNotListUnresolvedServerConfig() {
    // Given:
    final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        "some.random.property", "might be sensitive"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get("some.random.property"), is(nullValue()));
  }

  @Test
  public void shouldDefaultOptimizationsToOn() {
    // When:
    final KaypherConfig config = new KaypherConfig(Collections.emptyMap());

    // Then:
    assertThat(
        config.getKaypherStreamConfigProps().get(StreamsConfig.TOPOLOGY_OPTIMIZATION),
        equalTo(StreamsConfig.OPTIMIZE));
  }

  @Test
  public void shouldDefaultOptimizationsToOnForOldConfigs() {
    // When:
    final KaypherConfig config = new KaypherConfig(Collections.emptyMap())
        .overrideBreakingConfigsWithOriginalValues(Collections.emptyMap());

    // Then:
    assertThat(
        config.getKaypherStreamConfigProps().get(StreamsConfig.TOPOLOGY_OPTIMIZATION),
        equalTo(StreamsConfig.OPTIMIZE));
  }

  @Test
  public void shouldPreserveOriginalOptimizationConfig() {
    // Given:
    final KaypherConfig config = new KaypherConfig(
        Collections.singletonMap(
            StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE));
    final KaypherConfig saved = new KaypherConfig(
        Collections.singletonMap(
            StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION));

    // When:
    final KaypherConfig merged = config.overrideBreakingConfigsWithOriginalValues(
        saved.getAllConfigPropsWithSecretsObfuscated());

    // Then:
    assertThat(
        merged.getKaypherStreamConfigProps().get(StreamsConfig.TOPOLOGY_OPTIMIZATION),
        equalTo(StreamsConfig.NO_OPTIMIZATION));
  }

  @Test
  public void shouldFilterProducerConfigs() {
    // Given:
    final Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.CLIENT_ID_CONFIG, null);
    configs.put("not.a.config", "123");

    final KaypherConfig kaypherConfig = new KaypherConfig(configs);

    // When:
    assertThat(kaypherConfig.getProducerClientConfigProps(), hasEntry(ProducerConfig.ACKS_CONFIG, "all"));
    assertThat(kaypherConfig.getProducerClientConfigProps(), hasEntry(ProducerConfig.CLIENT_ID_CONFIG, null));
    assertThat(kaypherConfig.getProducerClientConfigProps(), not(hasKey("not.a.config")));
  }

  @Test
  public void shouldRaiseIfInternalTopicNamingOffAndStreamsOptimizationsOn() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Internal topic naming must be enabled if streams optimizations enabled");
    new KaypherConfig(
        ImmutableMap.of(
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS_OFF,
            StreamsConfig.TOPOLOGY_OPTIMIZATION,
            StreamsConfig.OPTIMIZE)
    );
  }

  @Test
  public void shouldRaiseOnInvalidInternalTopicNamingValue() {
    expectedException.expect(ConfigException.class);
    new KaypherConfig(
        Collections.singletonMap(
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS,
            "foobar"
        )
    );
  }
}
