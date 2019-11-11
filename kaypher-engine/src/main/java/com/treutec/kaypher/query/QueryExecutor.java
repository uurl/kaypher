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
package com.treutec.kaypher.query;

import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.errors.ProductionExceptionHandlerUtil;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.materialization.MaterializationInfo;
import com.treutec.kaypher.execution.materialization.MaterializationInfo.Builder;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.KStreamHolder;
import com.treutec.kaypher.execution.plan.KTableHolder;
import com.treutec.kaypher.execution.plan.PlanBuilder;
import com.treutec.kaypher.execution.streams.KSPlanBuilder;
import com.treutec.kaypher.execution.streams.materialization.KaypherMaterializationFactory;
import com.treutec.kaypher.execution.streams.materialization.MaterializationProvider;
import com.treutec.kaypher.execution.streams.materialization.ks.KsMaterialization;
import com.treutec.kaypher.execution.streams.materialization.ks.KsMaterializationFactory;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.logging.processing.NoopProcessingLogContext;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.logging.processing.ProcessingLogger;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metrics.ConsumerCollector;
import com.treutec.kaypher.metrics.ProducerCollector;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.serde.GenericKeySerDe;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KafkaStreamsUncaughtExceptionHandler;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import com.treutec.kaypher.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class QueryExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final KaypherConfig kaypherConfig;
  private final Map<String, Object> overrides;
  private final ProcessingLogContext processingLogContext;
  private final ServiceContext serviceContext;
  private final FunctionRegistry functionRegistry;
  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final Consumer<QueryMetadata> queryCloseCallback;
  private final KsMaterializationFactory ksMaterializationFactory;
  private final KaypherMaterializationFactory kaypherMaterializationFactory;
  private final StreamsBuilder streamsBuilder;

  public QueryExecutor(
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overrides,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final Consumer<QueryMetadata> queryCloseCallback) {
    this(
        kaypherConfig,
        overrides,
        processingLogContext,
        serviceContext,
        functionRegistry,
        queryCloseCallback,
        new KafkaStreamsBuilderImpl(
            Objects.requireNonNull(serviceContext, "serviceContext").getKafkaClientSupplier()),
        new StreamsBuilder(),
        new KaypherMaterializationFactory(
            Objects.requireNonNull(kaypherConfig, "kaypherConfig"),
            Objects.requireNonNull(functionRegistry, "functionRegistry"),
            Objects.requireNonNull(processingLogContext, "processingLogContext")
        ),
        new KsMaterializationFactory()
    );
  }

  QueryExecutor(
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overrides,
      final ProcessingLogContext processingLogContext,
      final ServiceContext serviceContext,
      final FunctionRegistry functionRegistry,
      final Consumer<QueryMetadata> queryCloseCallback,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final StreamsBuilder streamsBuilder,
      final KaypherMaterializationFactory kaypherMaterializationFactory,
      final KsMaterializationFactory ksMaterializationFactory) {
    this.kaypherConfig = Objects.requireNonNull(kaypherConfig, "kaypherConfig");
    this.overrides = Objects.requireNonNull(overrides, "overrides");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.queryCloseCallback = Objects.requireNonNull(
        queryCloseCallback,
        "queryCloseCallback"
    );
    this.ksMaterializationFactory = Objects.requireNonNull(
        ksMaterializationFactory,
        "ksMaterializationFactory"
    );
    this.kaypherMaterializationFactory = Objects.requireNonNull(
        kaypherMaterializationFactory,
        "kaypherMaterializationFactory"
    );
    this.kafkaStreamsBuilder = Objects.requireNonNull(kafkaStreamsBuilder);
    this.streamsBuilder = Objects.requireNonNull(streamsBuilder, "builder");
  }

  public TransientQueryMetadata buildTransientQuery(
      final String statementText,
      final QueryId queryId,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary,
      final LogicalSchema schema,
      final OptionalInt limit
  ) {
    final TransientQueryQueue queue = buildTransientQueryQueue(queryId, physicalPlan, limit);
    final String transientQueryPrefix =
        kaypherConfig.getString(KaypherConfig.KAYPHER_TRANSIENT_QUERY_NAME_PREFIX_CONFIG);
    final String applicationId = addTimeSuffix(getQueryApplicationId(
        getServiceId(),
        transientQueryPrefix,
        queryId
    ));
    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);
    final KafkaStreams streams =
        kafkaStreamsBuilder.buildKafkaStreams(streamsBuilder, streamsProperties);
    streams.setUncaughtExceptionHandler(new KafkaStreamsUncaughtExceptionHandler());
    return new TransientQueryMetadata(
        statementText,
        streams,
        schema,
        sources,
        queue::setLimitHandler,
        planSummary,
        queue.getQueue(),
        applicationId,
        streamsBuilder.build(),
        streamsProperties,
        overrides,
        queryCloseCallback
    );
  }

  private static Optional<MaterializationInfo> getMaterializationInfo(final Object result) {
    if (result instanceof KTableHolder) {
      return ((KTableHolder<?>) result).getMaterializationBuilder().map(Builder::build);
    }
    return Optional.empty();
  }

  public PersistentQueryMetadata buildQuery(
      final String statementText,
      final QueryId queryId,
      final DataSource<?> sinkDataSource,
      final Set<SourceName> sources,
      final ExecutionStep<?> physicalPlan,
      final String planSummary
  ) {
    final KaypherQueryBuilder kaypherQueryBuilder = queryBuilder(queryId);
    final PlanBuilder planBuilder = new KSPlanBuilder(kaypherQueryBuilder);
    final Object result = physicalPlan.build(planBuilder);
    final String persistanceQueryPrefix =
        kaypherConfig.getString(KaypherConfig.KAYPHER_PERSISTENT_QUERY_NAME_PREFIX_CONFIG);
    final String applicationId = getQueryApplicationId(
        getServiceId(),
        persistanceQueryPrefix,
        queryId
    );
    final Map<String, Object> streamsProperties = buildStreamsProperties(applicationId, queryId);
    final KafkaStreams streams =
        kafkaStreamsBuilder.buildKafkaStreams(streamsBuilder, streamsProperties);
    final Topology topology = streamsBuilder.build();
    final PhysicalSchema querySchema = PhysicalSchema.from(
        sinkDataSource.getSchema(),
        sinkDataSource.getSerdeOptions()
    );
    final Optional<MaterializationProvider> materializationBuilder = getMaterializationInfo(result)
        .flatMap(info -> buildMaterializationProvider(
            info,
            streams,
            querySchema,
            sinkDataSource.getKaypherTopic().getKeyFormat(),
            streamsProperties
        ));
    return new PersistentQueryMetadata(
        statementText,
        streams,
        querySchema,
        sources,
        sinkDataSource.getName(),
        planSummary,
        queryId,
        sinkDataSource.getDataSourceType(),
        materializationBuilder,
        applicationId,
        sinkDataSource.getKaypherTopic(),
        topology,
        kaypherQueryBuilder.getSchemas(),
        streamsProperties,
        overrides,
        queryCloseCallback
    );
  }

  private TransientQueryQueue buildTransientQueryQueue(
      final QueryId queryId,
      final ExecutionStep<?> physicalPlan,
      final OptionalInt limit) {
    final KaypherQueryBuilder kaypherQueryBuilder = queryBuilder(queryId);
    final PlanBuilder planBuilder = new KSPlanBuilder(kaypherQueryBuilder);
    final Object buildResult = physicalPlan.build(planBuilder);
    final KStream<?, GenericRow> kstream;
    if (buildResult instanceof KStreamHolder<?>) {
      kstream = ((KStreamHolder<?>) buildResult).getStream();
    } else if (buildResult instanceof KTableHolder<?>) {
      final KTable<?, GenericRow> ktable = ((KTableHolder<?>) buildResult).getTable();
      kstream = ktable.toStream();
    } else {
      throw new IllegalStateException("Unexpected type built from exection plan");
    }
    return new TransientQueryQueue(kstream, limit);
  }

  private KaypherQueryBuilder queryBuilder(final QueryId queryId) {
    return KaypherQueryBuilder.of(
        streamsBuilder,
        kaypherConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );
  }

  private String getServiceId() {
    return KaypherConstants.KAYPHER_INTERNAL_TOPIC_PREFIX
        + kaypherConfig.getString(KaypherConfig.KAYPHER_SERVICE_ID_CONFIG);
  }

  private Map<String, Object> buildStreamsProperties(
      final String applicationId,
      final QueryId queryId
  ) {
    final Map<String, Object> newStreamsProperties
        = new HashMap<>(kaypherConfig.getKaypherStreamConfigProps());
    newStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    final ProcessingLogger logger
        = processingLogContext.getLoggerFactory().getLogger(queryId.toString());
    newStreamsProperties.put(
        ProductionExceptionHandlerUtil.KAYPHER_PRODUCTION_ERROR_LOGGER,
        logger);

    updateListProperty(
        newStreamsProperties,
        StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ConsumerCollector.class.getCanonicalName()
    );
    updateListProperty(
        newStreamsProperties,
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ProducerCollector.class.getCanonicalName()
    );
    return newStreamsProperties;
  }

  private static String getQueryApplicationId(
      final String serviceId,
      final String queryPrefix,
      final QueryId queryId) {
    return serviceId + queryPrefix + queryId;
  }

  private static void updateListProperty(
      final Map<String, Object> properties,
      final String key,
      final Object value
  ) {
    final Object obj = properties.getOrDefault(key, new LinkedList<String>());
    final List<Object> valueList;
    // The property value is either a comma-separated string of class names, or a list of class
    // names
    if (obj instanceof String) {
      // If its a string just split it on the separator so we dont have to worry about adding a
      // separator
      final String asString = (String) obj;
      valueList = new LinkedList<>(Arrays.asList(asString.split("\\s*,\\s*")));
    } else if (obj instanceof List) {
      // The incoming list could be an instance of an immutable list. So we create a modifiable
      // List out of it to ensure that it is mutable.
      valueList = new LinkedList<>((List<?>) obj);
    } else {
      throw new KaypherException("Expecting list or string for property: " + key);
    }
    valueList.add(value);
    properties.put(key, valueList);
  }

  private static String addTimeSuffix(final String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
  }

  private Optional<MaterializationProvider> buildMaterializationProvider(
      final MaterializationInfo info,
      final KafkaStreams kafkaStreams,
      final PhysicalSchema schema,
      final KeyFormat keyFormat,
      final Map<String, Object> streamsProperties
  ) {
    final Serializer<Struct> keySerializer = new GenericKeySerDe().create(
        keyFormat.getFormatInfo(),
        schema.keySchema(),
        kaypherConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    ).serializer();

    final Optional<KsMaterialization> ksMaterialization = ksMaterializationFactory
        .create(
            info.stateStoreName(),
            kafkaStreams,
            info.getStateStoreSchema(),
            keySerializer,
            keyFormat.getWindowType(),
            streamsProperties
        );

    return ksMaterialization.map(ksMat -> (queryId, contextStacker) -> kaypherMaterializationFactory
        .create(
            ksMat,
            info,
            queryId,
            contextStacker
        ));
  }
}
