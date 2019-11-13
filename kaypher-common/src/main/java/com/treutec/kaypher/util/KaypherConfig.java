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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.config.ConfigItem;
import com.treutec.kaypher.config.KaypherConfigResolver;
import com.treutec.kaypher.errors.LogMetricAndContinueExceptionHandler;
import com.treutec.kaypher.errors.ProductionExceptionHandlerUtil;
import com.treutec.kaypher.model.SemanticVersion;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;

public class KaypherConfig extends AbstractConfig {

  public static final String KAYPHER_CONFIG_PROPERTY_PREFIX = "kaypher.";

  public static final String KAYPHER_FUNCTIONS_PROPERTY_PREFIX =
      KAYPHER_CONFIG_PROPERTY_PREFIX + "functions.";

  static final String KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX =
      KAYPHER_FUNCTIONS_PROPERTY_PREFIX + "_global_.";

  public static final String METRIC_REPORTER_CLASSES_CONFIG = "kaypher.metric.reporters";

  public static final String METRIC_REPORTER_CLASSES_DOC =
      CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;

  public static final String SINK_NUMBER_OF_PARTITIONS_PROPERTY = "kaypher.sink.partitions";

  public static final String SINK_NUMBER_OF_REPLICAS_PROPERTY = "kaypher.sink.replicas";

  public static final String KAYPHER_INTERNAL_TOPIC_REPLICAS_PROPERTY = "kaypher.internal.topic.replicas";

  public static final String KAYPHER_SCHEMA_REGISTRY_PREFIX = "kaypher.schema.registry.";

  public static final String SCHEMA_REGISTRY_URL_PROPERTY = "kaypher.schema.registry.url";

  public static final String CONNECT_URL_PROPERTY = "kaypher.connect.url";

  public static final String CONNECT_WORKER_CONFIG_FILE_PROPERTY = "kaypher.connect.worker.config";

  public static final String KAYPHER_ENABLE_UDFS = "kaypher.udfs.enabled";

  public static final String KAYPHER_EXT_DIR = "kaypher.extension.dir";

  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY =
      "kaypher.sink.window.change.log.additional.retention";

  public static final String
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG = "kaypher.fail.on.deserialization.error";

  public static final String FAIL_ON_PRODUCTION_ERROR_CONFIG = "kaypher.fail.on.production.error";

  public static final String
      KAYPHER_SERVICE_ID_CONFIG = "kaypher.service.id";
  public static final String
      KAYPHER_SERVICE_ID_DEFAULT = "default_";

  public static final String
      KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG = "kaypher.persistent.prefix";
  public static final String
      KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT = "query_";

  public static final String
      KAYPHER_TRANSIENT_QUERY_NAME_PREFIX_CONFIG = "kaypher.transient.prefix";
  public static final String
      KAYPHER_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT = "transient_";

  public static final String
      KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_CONFIG = "kaypher.output.topic.name.prefix";
  private static final String KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_DOCS =
      "A prefix to add to any output topic names, where the statement does not include an explicit "
      + "topic name. E.g. given 'kaypher.output.topic.name.prefix = \"thing-\"', then statement "
      + "'CREATE STREAM S AS ...' will create a topic 'thing-S', where as the statement "
      + "'CREATE STREAM S WITH(KAFKA_TOPIC = 'foo') AS ...' will create a topic 'foo'.";

  public static final String KAYPHER_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG =
      KAYPHER_FUNCTIONS_PROPERTY_PREFIX + "substring.legacy.args";
  private static final String
      KAYPHER_FUNCTIONS_SUBSTRING_LEGACY_ARGS_DOCS = "Switch the SUBSTRING function into legacy mode,"
      + " i.e. back to how it was in version 5.0 and earlier of KAYPHER."
      + " Up to version 5.0.x substring took different args:"
      + " VARCHAR SUBSTRING(str VARCHAR, startIndex INT, endIndex INT), where startIndex and"
      + " endIndex were both base-zero indexed, e.g. a startIndex of '0' selected the start of the"
      + " string, and the last argument is a character index, rather than the length of the"
      + " substring to extract. Later versions of KAYPHER use:"
      + " VARCHAR SUBSTRING(str VARCHAR, pos INT, length INT), where pos is base-one indexed,"
      + " and the last argument is the length of the substring to extract.";

  public static final String KAYPHER_WINDOWED_SESSION_KEY_LEGACY_CONFIG =
      KAYPHER_CONFIG_PROPERTY_PREFIX + "windowed.session.key.legacy";

  private static final String KAYPHER_WINDOWED_SESSION_KEY_LEGACY_DOC = ""
      + "Version 5.1 of KAYPHER and earlier incorrectly excluded the end time in the record key in "
      + "Kafka for session windowed data. Setting this value to true will make KAYPHER expect and "
      + "continue to store session keys without the end time. With the default value of false "
      + "new queries will now correctly store the session end time as part of the key";

  public static final String KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG =
      "kaypher.query.persistent.active.limit";
  private static final int KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT = Integer.MAX_VALUE;
  private static final String KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC =
      "An upper limit on the number of active, persistent queries that may be running at a time, "
      + "in interactive mode. Once this limit is reached, any further persistent queries will not "
      + "be accepted.";

  public static final String KAYPHER_USE_NAMED_INTERNAL_TOPICS = "kaypher.named.internal.topics";
  private static final String KAYPHER_USE_NAMED_INTERNAL_TOPICS_DOC = "";
  public static final String KAYPHER_USE_NAMED_INTERNAL_TOPICS_ON = "on";
  public static final String KAYPHER_USE_NAMED_INTERNAL_TOPICS_OFF = "off";
  private static final Validator KAYPHER_USE_NAMED_INTERNAL_TOPICS_VALIDATOR = ValidString.in(
      KAYPHER_USE_NAMED_INTERNAL_TOPICS_ON, KAYPHER_USE_NAMED_INTERNAL_TOPICS_OFF
  );

  public static final String KAYPHER_USE_NAMED_AVRO_MAPS = "kaypher.avro.maps.named";
  private static final String KAYPHER_USE_NAMED_AVRO_MAPS_DOC = "";

  public static final String KAYPHER_LEGACY_REPARTITION_ON_GROUP_BY_ROWKEY =
      "kaypher.query.stream.groupby.rowkey.repartition";
  public static final String KAYPHER_INJECT_LEGACY_MAP_VALUES_NODE =
      "kaypher.query.inject.legacy.map.values.node";

  public static final String KAYPHER_WRAP_SINGLE_VALUES =
      "kaypher.persistence.wrap.single.values";

  public static final String KAYPHER_CUSTOM_METRICS_TAGS = "kaypher.metrics.tags.custom";
  private static final String KAYPHER_CUSTOM_METRICS_TAGS_DOC =
      "A list of tags to be included with emitted JMX metrics, formatted as a string of key:value "
      + "pairs separated by commas. For example, 'key1:value1,key2:value2'.";

  public static final String KAYPHER_CUSTOM_METRICS_EXTENSION = "kaypher.metrics.extension";
  private static final String KAYPHER_CUSTOM_METRICS_EXTENSION_DOC =
      "Extension for supplying custom metrics to be emitted along with "
      + "the engine's default JMX metrics";

  public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
  public static final String DEFAULT_CONNECT_URL = "http://localhost:8083";

  public static final String KAYPHER_STREAMS_PREFIX = "kaypher.streams.";

  public static final String KAYPHER_COLLECT_UDF_METRICS = "kaypher.udf.collect.metrics";
  public static final String KAYPHER_UDF_SECURITY_MANAGER_ENABLED = "kaypher.udf.enable.security.manager";

  public static final String KAYPHER_INSERT_INTO_VALUES_ENABLED = "kaypher.insert.into.values.enabled";

  public static final String DEFAULT_EXT_DIR = "ext";

  public static final String KAYPHER_SECURITY_EXTENSION_CLASS = "kaypher.security.extension.class";
  public static final String KAYPHER_SECURITY_EXTENSION_DEFAULT = null;
  public static final String KAYPHER_SECURITY_EXTENSION_DOC = "A KAYPHER security extension class that "
      + "provides authorization to KAYPHER servers.";

  public static final String KAYPHER_ENABLE_TOPIC_ACCESS_VALIDATOR = "kaypher.access.validator.enable";
  public static final String KAYPHER_ACCESS_VALIDATOR_ON = "on";
  public static final String KAYPHER_ACCESS_VALIDATOR_OFF = "off";
  public static final String KAYPHER_ACCESS_VALIDATOR_AUTO = "auto";
  public static final String KAYPHER_ACCESS_VALIDATOR_DOC =
      "Config to enable/disable the topic access validator, which checks that KAYPHER can access "
          + "the involved topics before committing to execute a statement. Possible values are "
          + "\"on\", \"off\", and \"auto\". Setting to \"on\" enables the validator. Setting to "
          + "\"off\" disables the validator. If set to \"auto\", KAYPHER will attempt to discover "
          + "whether the Kafka cluster supports the required API, and enables the validator if "
          + "it does.";

  public static final String KAYPHER_PULL_QUERIES_ENABLE_CONFIG = "kaypher.pull.queries.enable";
  public static final String KAYPHER_PULL_QUERIES_ENABLE_DOC =
      "Config to enable or disable transient pull queries on a specific KAYPHER server.";
  public static final boolean KAYPHER_PULL_QUERIES_ENABLE_DEFAULT = true;

  public static final Collection<CompatibilityBreakingConfigDef> COMPATIBLY_BREAKING_CONFIG_DEFS
      = ImmutableList.of(
          new CompatibilityBreakingConfigDef(
              KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG,
              ConfigDef.Type.STRING,
              KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
              KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT,
              ConfigDef.Importance.MEDIUM,
              Optional.empty(),
              "Second part of the prefix for persistent queries. For instance if "
                  + "the prefix is query_ the query name will be kaypher_query_1."),
          new CompatibilityBreakingConfigDef(
              KAYPHER_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG,
              ConfigDef.Type.BOOLEAN,
              false,
              false,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              KAYPHER_FUNCTIONS_SUBSTRING_LEGACY_ARGS_DOCS),
          new CompatibilityBreakingConfigDef(
              KAYPHER_WINDOWED_SESSION_KEY_LEGACY_CONFIG,
              ConfigDef.Type.BOOLEAN,
              false,
              false,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              KAYPHER_WINDOWED_SESSION_KEY_LEGACY_DOC),
          new CompatibilityBreakingConfigDef(
              KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
              ConfigDef.Type.INT,
              KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT,
              KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC),
          new CompatibilityBreakingConfigDef(
              KAYPHER_USE_NAMED_INTERNAL_TOPICS,
              ConfigDef.Type.STRING,
              KAYPHER_USE_NAMED_INTERNAL_TOPICS_ON,
              KAYPHER_USE_NAMED_INTERNAL_TOPICS_ON,
              ConfigDef.Importance.LOW,
              KAYPHER_USE_NAMED_INTERNAL_TOPICS_DOC,
              Optional.empty(),
              KAYPHER_USE_NAMED_INTERNAL_TOPICS_VALIDATOR),
          new CompatibilityBreakingConfigDef(
              SINK_NUMBER_OF_PARTITIONS_PROPERTY,
              Type.INT,
              null,
              null,
              Importance.LOW,
              Optional.empty(),
              "The legacy default number of partitions for the topics created by KAYPHER"
                  + "in 5.2 and earlier versions."
                  + "This property should not be set for 5.3 and later versions."),
          new CompatibilityBreakingConfigDef(
              SINK_NUMBER_OF_REPLICAS_PROPERTY,
              ConfigDef.Type.SHORT,
              null,
              null,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              "The default number of replicas for the topics created by KAYPHER "
                  + "in 5.2 and earlier versions."
                  + "This property should not be set for 5.3 and later versions."
          ),
          new CompatibilityBreakingConfigDef(
              KAYPHER_USE_NAMED_AVRO_MAPS,
              ConfigDef.Type.BOOLEAN,
              true,
              true,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              KAYPHER_USE_NAMED_AVRO_MAPS_DOC
          ),
          new CompatibilityBreakingConfigDef(
              KAYPHER_LEGACY_REPARTITION_ON_GROUP_BY_ROWKEY,
              ConfigDef.Type.BOOLEAN,
              false,
              false,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              "Ensures legacy queries that perform a 'GROUP BY ROWKEY' continue to "
                  + "perform an unnecessary repartition step"
          ),
          new CompatibilityBreakingConfigDef(
              KAYPHER_INJECT_LEGACY_MAP_VALUES_NODE,
              ConfigDef.Type.BOOLEAN,
              false,
              false,
              ConfigDef.Importance.LOW,
              Optional.empty(),
              "Ensures legacy queries maintian the same topology"
          )
  );

  private enum ConfigGeneration {
    LEGACY,
    CURRENT
  }

  public static class CompatibilityBreakingConfigDef {
    private final String name;
    private final ConfigDef.Type type;
    private final Object defaultValueLegacy;
    private final Object defaultValueCurrent;
    private final ConfigDef.Importance importance;
    private final String documentation;
    private final Optional<SemanticVersion> since;
    private final Validator validator;

    CompatibilityBreakingConfigDef(
        final String name,
        final ConfigDef.Type type,
        final Object defaultValueLegacy,
        final Object defaultValueCurrent,
        final ConfigDef.Importance importance,
        final Optional<SemanticVersion> since,
        final String documentation
    ) {
      this(
          name,
          type,
          defaultValueLegacy,
          defaultValueCurrent,
          importance,
          documentation,
          since,
          null);
    }

    CompatibilityBreakingConfigDef(
        final String name,
        final ConfigDef.Type type,
        final Object defaultValueLegacy,
        final Object defaultValueCurrent,
        final ConfigDef.Importance importance,
        final String documentation,
        final Optional<SemanticVersion> since,
        final Validator validator
    ) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
      this.defaultValueLegacy = defaultValueLegacy;
      this.defaultValueCurrent = defaultValueCurrent;
      this.importance = Objects.requireNonNull(importance, "importance");
      this.documentation = Objects.requireNonNull(documentation, "documentation");
      this.since = Objects.requireNonNull(since, "since");
      this.validator = validator;
    }

    public String getName() {
      return this.name;
    }

    public Optional<SemanticVersion> since() {
      return since;
    }

    public Object getCurrentDefaultValue() {
      return defaultValueCurrent;
    }

    private void define(final ConfigDef configDef, final Object defaultValue) {
      configDef.define(name, type, defaultValue, validator, importance, documentation);
    }

    void defineLegacy(final ConfigDef configDef) {
      define(configDef, defaultValueLegacy);
    }

    void defineCurrent(final ConfigDef configDef) {
      define(configDef, defaultValueCurrent);
    }
  }

  private static final Collection<CompatibilityBreakingStreamsConfig>
      COMPATIBILITY_BREAKING_STREAMS_CONFIGS = ImmutableList.of(
          new CompatibilityBreakingStreamsConfig(
              StreamsConfig.TOPOLOGY_OPTIMIZATION,
              StreamsConfig.OPTIMIZE,
              StreamsConfig.OPTIMIZE)
  );

  private static final class CompatibilityBreakingStreamsConfig {
    final String name;
    final Object defaultValueLegacy;
    final Object defaultValueCurrent;

    CompatibilityBreakingStreamsConfig(final String name, final Object defaultValueLegacy,
        final Object defaultValueCurrent) {
      this.name = Objects.requireNonNull(name);
      if (!StreamsConfig.configDef().names().contains(name)) {
        throw new IllegalArgumentException(
            String.format("%s is not a valid streams config", name));
      }
      this.defaultValueLegacy = defaultValueLegacy;
      this.defaultValueCurrent = defaultValueCurrent;
    }

    String getName() {
      return this.name;
    }
  }

  public static final ConfigDef CURRENT_DEF = buildConfigDef(ConfigGeneration.CURRENT);
  public static final ConfigDef LEGACY_DEF = buildConfigDef(ConfigGeneration.LEGACY);
  public static final Set<String> SSL_CONFIG_NAMES = sslConfigNames();

  private static ConfigDef configDef(final ConfigGeneration generation) {
    return generation == ConfigGeneration.CURRENT ? CURRENT_DEF : LEGACY_DEF;
  }

  // CHECKSTYLE_RULES.OFF: MethodLength
  private static ConfigDef buildConfigDef(final ConfigGeneration generation) {
    final ConfigDef configDef = new ConfigDef()
        .define(
            KAYPHER_SERVICE_ID_CONFIG,
            ConfigDef.Type.STRING,
            KAYPHER_SERVICE_ID_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Indicates the ID of the kaypher service. It will be used as prefix for "
                + "all implicitly named resources created by this instance in Kafka. "
                + "By convention, the id should end in a seperator character of some form, e.g. "
                + "a dash or underscore, as this makes identifiers easier to read."
        )
        .define(
            KAYPHER_TRANSIENT_QUERY_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            KAYPHER_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Second part of the prefix for transient queries. For instance if "
            + "the prefix is transient_ the query name would be "
            + "kaypher_transient_4120896722607083946_1509389010601 where 'kaypher_' is the first prefix"
            + " and '_transient' is the second part of the prefix for the query id the third and "
            + "4th parts are a random long value and the current timestamp. "
        ).define(
            KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_DOCS
        ).define(
            SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
            ConfigDef.Type.LONG,
            KaypherConstants.defaultSinkWindowChangeLogAdditionalRetention,
            ConfigDef.Importance.MEDIUM,
            "The default window change log additional retention time. This "
            + "is a streams config value which will be added to a windows maintainMs to ensure "
            + "data is not deleted from the log prematurely. Allows for clock drift. "
            + "Default is 1 day"
        ).define(
            SCHEMA_REGISTRY_URL_PROPERTY,
            ConfigDef.Type.STRING,
            DEFAULT_SCHEMA_REGISTRY_URL,
            ConfigDef.Importance.MEDIUM,
            "The URL for the schema registry, defaults to http://localhost:8081"
        ).define(
            CONNECT_URL_PROPERTY,
            ConfigDef.Type.STRING,
            DEFAULT_CONNECT_URL,
            Importance.MEDIUM,
            "The URL for the connect deployment, defaults to http://localhost:8083"
        ).define(
            CONNECT_WORKER_CONFIG_FILE_PROPERTY,
            ConfigDef.Type.STRING,
            "",
            Importance.LOW,
            "The path to a connect worker configuration file. An empty value for this configuration"
                + "will prevent connect from starting up embedded within KAYPHER. For more information"
                + " on configuring connect, see "
                + "https://docs.confluent.io/current/connect/userguide.html#configuring-workers."
        ).define(
            KAYPHER_ENABLE_UDFS,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            "Whether or not custom UDF jars found in the ext dir should be loaded. Default is true "
        ).define(
            KAYPHER_COLLECT_UDF_METRICS,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.LOW,
            "Whether or not metrics should be collected for custom udfs. Default is false. Note: "
                + "this will add some overhead to udf invocation. It is recommended that this "
                + " be set to false in production."
        ).define(
            KAYPHER_EXT_DIR,
            ConfigDef.Type.STRING,
            DEFAULT_EXT_DIR,
            ConfigDef.Importance.LOW,
            "The path to look for and load extensions such as UDFs from."
        ).define(
            KAYPHER_INTERNAL_TOPIC_REPLICAS_PROPERTY,
            Type.SHORT,
            (short) 1,
            ConfigDef.Importance.LOW,
            "The replication factor for the internal topics of KAYPHER server."
        ).define(
            KAYPHER_UDF_SECURITY_MANAGER_ENABLED,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            "Enable the security manager for UDFs. Default is true and will stop UDFs from"
               + " calling System.exit or executing processes"
        ).define(
            KAYPHER_INSERT_INTO_VALUES_ENABLED,
            Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            "Enable the INSERT INTO ... VALUES functionality."
        ).define(
            KAYPHER_SECURITY_EXTENSION_CLASS,
            Type.CLASS,
            KAYPHER_SECURITY_EXTENSION_DEFAULT,
            ConfigDef.Importance.LOW,
            KAYPHER_SECURITY_EXTENSION_DOC
        ).define(
            KAYPHER_WRAP_SINGLE_VALUES,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.LOW,
            "Controls how KAYPHER will serialize a value whose schema contains only a "
                + "single column. The setting only sets the default for `CREATE STREAM`, "
                + "`CREATE TABLE`, `CREATE STREAM AS SELECT`, `CREATE TABLE AS SELECT` and "
                + "`INSERT INTO` statements, where `WRAP_SINGLE_VALUE` is not provided explicitly "
                + "in the statement." + System.lineSeparator()
                + "When set to true, KAYPHER will persist the single column nested with a STRUCT, "
                + "for formats that support them. When set to false KAYPHER will persist "
                + "the column as the anonymous values." + System.lineSeparator()
                + "For example, if the value contains only a single column 'FOO INT' and the "
                + "format is JSON,  and this setting is `false`, then KAYPHER will persist the value "
                + "as an unnamed JSON number, e.g. '10'. Where as, if this setting is `true`, KAYPHER "
                + "will persist the value as a JSON document with a single numeric property, "
                + "e.g. '{\"FOO\": 10}." + System.lineSeparator()
                + "Note: the DELIMITED format ignores this setting as it does not support the "
                + "concept of a STRUCT, record or object."
        ).define(
            KAYPHER_CUSTOM_METRICS_TAGS,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            KAYPHER_CUSTOM_METRICS_TAGS_DOC
        ).define(
            KAYPHER_CUSTOM_METRICS_EXTENSION,
            ConfigDef.Type.CLASS,
            null,
            ConfigDef.Importance.LOW,
            KAYPHER_CUSTOM_METRICS_EXTENSION_DOC
        ).define(
            KAYPHER_ENABLE_TOPIC_ACCESS_VALIDATOR,
            Type.STRING,
            KAYPHER_ACCESS_VALIDATOR_AUTO,
            ValidString.in(
                KAYPHER_ACCESS_VALIDATOR_ON,
                KAYPHER_ACCESS_VALIDATOR_OFF,
                KAYPHER_ACCESS_VALIDATOR_AUTO
            ),
            ConfigDef.Importance.LOW,
            KAYPHER_ACCESS_VALIDATOR_DOC
        ).define(METRIC_REPORTER_CLASSES_CONFIG,
            Type.LIST,
            "",
            Importance.LOW,
            METRIC_REPORTER_CLASSES_DOC
        ).define(
            KAYPHER_PULL_QUERIES_ENABLE_CONFIG,
            Type.BOOLEAN,
            KAYPHER_PULL_QUERIES_ENABLE_DEFAULT,
            Importance.LOW,
            KAYPHER_PULL_QUERIES_ENABLE_DOC
        )
        .withClientSslSupport();
    for (final CompatibilityBreakingConfigDef compatibilityBreakingConfigDef
        : COMPATIBLY_BREAKING_CONFIG_DEFS) {
      if (generation == ConfigGeneration.CURRENT) {
        compatibilityBreakingConfigDef.defineCurrent(configDef);
      } else {
        compatibilityBreakingConfigDef.defineLegacy(configDef);
      }
    }
    return configDef;
  }
  // CHECKSTYLE_RULES.ON: MethodLength

  private static final class ConfigValue {
    final ConfigItem configItem;
    final String key;
    final Object value;

    private ConfigValue(final ConfigItem configItem, final String key, final Object value) {
      this.configItem = configItem;
      this.key = key;
      this.value = value;
    }

    private boolean isResolved() {
      return configItem.isResolved();
    }

    private String convertToObfuscatedString() {
      return configItem.convertToString(value);
    }
  }

  private static void applyStreamsConfig(
      final Map<String, ?> props,
      final Map<String, ConfigValue> streamsConfigProps) {
    props.entrySet()
        .stream()
        .map(e -> resolveStreamsConfig(e.getKey(), e.getValue()))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            configValue -> streamsConfigProps.put(configValue.key, configValue));
  }

  private static Optional<ConfigValue> resolveStreamsConfig(
      final String maybePrefixedKey,
      final Object value) {
    final String key = maybePrefixedKey.startsWith(KAYPHER_STREAMS_PREFIX)
        ? maybePrefixedKey.substring(KAYPHER_STREAMS_PREFIX.length()) : maybePrefixedKey;

    if (key.startsWith(KaypherConfig.KAYPHER_CONFIG_PROPERTY_PREFIX)) {
      return Optional.empty();
    }

    return new KaypherConfigResolver().resolve(maybePrefixedKey, false)
        .map(configItem -> new ConfigValue(configItem, key, configItem.parseValue(value)));
  }

  private static Map<String, ConfigValue> buildStreamingConfig(
      final Map<String, ?> baseStreamConfig,
      final Map<String, ?> overrides) {
    final Map<String, ConfigValue> streamConfigProps = new HashMap<>();
    applyStreamsConfig(baseStreamConfig, streamConfigProps);
    applyStreamsConfig(overrides, streamConfigProps);
    return ImmutableMap.copyOf(streamConfigProps);
  }

  private final Map<String, ConfigValue> kaypherStreamConfigProps;

  public KaypherConfig(final Map<?, ?> props) {
    this(ConfigGeneration.CURRENT, props);
  }

  private KaypherConfig(final ConfigGeneration generation, final Map<?, ?> props) {
    super(configDef(generation), props);

    final Map<String, Object> streamsConfigDefaults = new HashMap<>();
    streamsConfigDefaults.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KaypherConstants
        .defaultAutoOffsetRestConfig);
    streamsConfigDefaults.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KaypherConstants
        .defaultCommitIntervalMsConfig);
    streamsConfigDefaults.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, KaypherConstants
            .defaultCacheMaxBytesBufferingConfig);
    streamsConfigDefaults.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, KaypherConstants
        .defaultNumberOfStreamsThreads);
    if (!getBooleanConfig(FAIL_ON_DESERIALIZATION_ERROR_CONFIG, false)) {
      streamsConfigDefaults.put(
          StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogMetricAndContinueExceptionHandler.class
      );
    }
    streamsConfigDefaults.put(
        StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        ProductionExceptionHandlerUtil.getHandler(
            getBooleanConfig(FAIL_ON_PRODUCTION_ERROR_CONFIG, true))
    );
    COMPATIBILITY_BREAKING_STREAMS_CONFIGS.forEach(
        config -> streamsConfigDefaults.put(
            config.name,
            generation == ConfigGeneration.CURRENT
                ? config.defaultValueCurrent : config.defaultValueLegacy));
    this.kaypherStreamConfigProps = buildStreamingConfig(streamsConfigDefaults, originals());
    validate();
  }

  private boolean getBooleanConfig(final String config, final boolean defaultValue) {
    final Object value = originals().get(config);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value.toString());
  }

  private KaypherConfig(final ConfigGeneration generation,
                     final Map<String, ?> values,
                     final Map<String, ConfigValue> kaypherStreamConfigProps) {
    super(configDef(generation), values);
    this.kaypherStreamConfigProps = kaypherStreamConfigProps;
  }

  private void validate() {
    final Object optimizationsConfig = getKaypherStreamConfigProps().get(
        StreamsConfig.TOPOLOGY_OPTIMIZATION);
    final Object useInternalNamesConfig = get(KAYPHER_USE_NAMED_INTERNAL_TOPICS);
    if (Objects.equals(optimizationsConfig, StreamsConfig.OPTIMIZE)
        && useInternalNamesConfig.equals(KAYPHER_USE_NAMED_INTERNAL_TOPICS_OFF)) {
      throw new RuntimeException(
          "Internal topic naming must be enabled if streams optimizations enabled");
    }
  }

  public Map<String, Object> getKaypherStreamConfigProps() {
    final Map<String, Object> props = new HashMap<>();
    for (final ConfigValue config : kaypherStreamConfigProps.values()) {
      props.put(config.key, config.value);
    }
    return Collections.unmodifiableMap(props);
  }

  public Map<String, Object> getKaypherAdminClientConfigProps() {
    return getConfigsFor(AdminClientConfig.configNames());
  }

  public Map<String, Object> getProducerClientConfigProps() {
    return getConfigsFor(ProducerConfig.configNames());
  }

  private Map<String, Object> getConfigsFor(final Set<String> configs) {
    final Map<String, Object> props = new HashMap<>();
    kaypherStreamConfigProps.values().stream()
        .filter(configValue -> configs.contains(configValue.key))
        .forEach(configValue -> props.put(configValue.key, configValue.value));
    return Collections.unmodifiableMap(props);
  }

  public Map<String, Object> getKaypherFunctionsConfigProps(final String functionName) {
    final Map<String, Object> udfProps = originalsWithPrefix(
        KAYPHER_FUNCTIONS_PROPERTY_PREFIX + functionName.toLowerCase(), false);

    final Map<String, Object> globals = originalsWithPrefix(
        KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX, false);

    udfProps.putAll(globals);

    return udfProps;
  }

  private Map<String, String> getKaypherConfigPropsWithSecretsObfuscated() {
    final Map<String, String> props = new HashMap<>();

    originalsWithPrefix(KAYPHER_FUNCTIONS_PROPERTY_PREFIX, false)
        .forEach((key, value) -> props.put(key, "[hidden]"));

    configDef(ConfigGeneration.CURRENT).names().stream()
        .filter(key -> !SSL_CONFIG_NAMES.contains(key))
        .forEach(
            key -> props.put(key, ConfigDef.convertToString(values().get(key), typeOf(key))));

    return Collections.unmodifiableMap(props);
  }

  private Map<String, String> getKaypherStreamConfigPropsWithSecretsObfuscated() {
    final Map<String, String> props = new HashMap<>();
    // build a properties map with obfuscated values for sensitive configs.
    // Obfuscation is handled by ConfigDef.convertToString
    kaypherStreamConfigProps.values().stream()
        // we must only return props for which we could resolve
        .filter(ConfigValue::isResolved)
        .forEach(
            configValue -> props.put(
                configValue.key,
                configValue.convertToObfuscatedString()));
    return Collections.unmodifiableMap(props);
  }

  public Map<String, String> getAllConfigPropsWithSecretsObfuscated() {
    final Map<String, String> allPropsCleaned = new HashMap<>();
    // build a properties map with obfuscated values for sensitive configs.
    // Obfuscation is handled by ConfigDef.convertToString
    allPropsCleaned.putAll(getKaypherConfigPropsWithSecretsObfuscated());
    allPropsCleaned.putAll(
        getKaypherStreamConfigPropsWithSecretsObfuscated().entrySet().stream().collect(
            Collectors.toMap(
                e -> KAYPHER_STREAMS_PREFIX + e.getKey(), Map.Entry::getValue
            )
        )
    );
    return Collections.unmodifiableMap(allPropsCleaned);
  }

  public KaypherConfig cloneWithPropertyOverwrite(final Map<String, ?> props) {
    final Map<String, Object> cloneProps = new HashMap<>(originals());
    cloneProps.putAll(props);
    final Map<String, ConfigValue> streamConfigProps =
        buildStreamingConfig(getKaypherStreamConfigProps(), props);

    return new KaypherConfig(ConfigGeneration.CURRENT, cloneProps, streamConfigProps);
  }

  public KaypherConfig overrideBreakingConfigsWithOriginalValues(final Map<String, ?> props) {
    final KaypherConfig originalConfig = new KaypherConfig(ConfigGeneration.LEGACY, props);
    final Map<String, Object> mergedProperties = new HashMap<>(originals());
    COMPATIBLY_BREAKING_CONFIG_DEFS.stream()
        .map(CompatibilityBreakingConfigDef::getName)
        .forEach(
            k -> mergedProperties.put(k, originalConfig.get(k)));
    final Map<String, ConfigValue> mergedStreamConfigProps
        = new HashMap<>(this.kaypherStreamConfigProps);
    COMPATIBILITY_BREAKING_STREAMS_CONFIGS.stream()
        .map(CompatibilityBreakingStreamsConfig::getName)
        .forEach(
            k -> mergedStreamConfigProps.put(k, originalConfig.kaypherStreamConfigProps.get(k)));
    return new KaypherConfig(ConfigGeneration.LEGACY, mergedProperties, mergedStreamConfigProps);
  }

  public Map<String, String> getStringAsMap(final String key) {
    final String value = getString(key).trim();
    try {
      return value.equals("")
          ? Collections.emptyMap()
          : Splitter.on(",").trimResults().withKeyValueSeparator(":").split(value);
    } catch (IllegalArgumentException e) {
      throw new KaypherException(
          String.format(
              "Invalid config value for '%s'. value: %s. reason: %s",
              key,
              value,
              e.getMessage()));
    }
  }

  private static Set<String> sslConfigNames() {
    final ConfigDef sslConfig = new ConfigDef();
    SslConfigs.addClientSslSupport(sslConfig);
    return sslConfig.names();
  }
}
