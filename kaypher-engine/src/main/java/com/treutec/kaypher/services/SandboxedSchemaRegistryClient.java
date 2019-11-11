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

import static com.treutec.kaypher.util.LimitedProxyBuilder.anyParams;
import static com.treutec.kaypher.util.LimitedProxyBuilder.methodParams;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.util.LimitedProxyBuilder;
import java.util.Collections;
import java.util.Objects;
import org.apache.avro.Schema;

/**
 * SchemaRegistryClient used when trying out operations.
 *
 * <p>The client will not allow any operation that changes the state of the Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedSchemaRegistryClient {

  static SchemaRegistryClient createProxy(final SchemaRegistryClient delegate) {
    Objects.requireNonNull(delegate, "delegate");

    return LimitedProxyBuilder.forClass(SchemaRegistryClient.class)
        .swallow("register", anyParams(), 123)
        .forward("getLatestSchemaMetadata", methodParams(String.class), delegate)
        .forward("testCompatibility",
            methodParams(String.class, Schema.class), delegate)
        .swallow("deleteSubject", methodParams(String.class), Collections.emptyList())
        .build();
  }

  private SandboxedSchemaRegistryClient() {
  }
}
