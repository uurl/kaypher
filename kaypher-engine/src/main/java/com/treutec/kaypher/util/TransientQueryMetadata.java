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

import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.query.LimitHandler;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;

/**
 * Metadata of a transient query, e.g. {@code SELECT * FROM FOO;}.
 */
public class TransientQueryMetadata extends QueryMetadata {

  private final BlockingQueue<KeyValue<String, GenericRow>> rowQueue;
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final Consumer<LimitHandler> limitHandlerSetter;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public TransientQueryMetadata(
      final String statementString,
      final KafkaStreams kafkaStreams,
      final LogicalSchema logicalSchema,
      final Set<SourceName> sourceNames,
      final Consumer<LimitHandler> limitHandlerSetter,
      final String executionPlan,
      final BlockingQueue<KeyValue<String, GenericRow>> rowQueue,
      final String queryApplicationId,
      final Topology topology,
      final Map<String, Object> streamsProperties,
      final Map<String, Object> overriddenProperties,
      final Consumer<QueryMetadata> closeCallback) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        statementString,
        kafkaStreams,
        logicalSchema,
        sourceNames,
        executionPlan,
        queryApplicationId,
        topology,
        streamsProperties,
        overriddenProperties,
        closeCallback
    );
    this.limitHandlerSetter = Objects.requireNonNull(limitHandlerSetter, "limitHandlerSetter");
    this.rowQueue = Objects.requireNonNull(rowQueue, "rowQueue");
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public BlockingQueue<KeyValue<String, GenericRow>> getRowQueue() {
    return rowQueue;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof TransientQueryMetadata)) {
      return false;
    }

    final TransientQueryMetadata that = (TransientQueryMetadata) o;

    return Objects.equals(this.rowQueue, that.rowQueue) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowQueue, super.hashCode());
  }

  public void setLimitHandler(final LimitHandler limitHandler) {
    limitHandlerSetter.accept(limitHandler);
  }

  @Override
  public void close() {
    super.close();
    isRunning.set(false);
  }
}
