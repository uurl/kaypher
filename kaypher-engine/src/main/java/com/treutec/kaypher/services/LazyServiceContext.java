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

import com.google.common.base.Suppliers;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.streams.KafkaClientSupplier;

public class LazyServiceContext implements ServiceContext {
  private final Supplier<ServiceContext> serviceContextSupplier;

  public LazyServiceContext(final Supplier<ServiceContext> serviceContextSupplier) {
    this.serviceContextSupplier = Suppliers.memoize(serviceContextSupplier::get)::get;
  }

  @Override
  public Admin getAdminClient() {
    return serviceContextSupplier.get().getAdminClient();
  }

  @Override
  public KafkaTopicClient getTopicClient() {
    return serviceContextSupplier.get().getTopicClient();
  }

  @Override
  public KafkaClientSupplier getKafkaClientSupplier() {
    return serviceContextSupplier.get().getKafkaClientSupplier();
  }

  @Override
  public SchemaRegistryClient getSchemaRegistryClient() {
    return serviceContextSupplier.get().getSchemaRegistryClient();
  }

  @Override
  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return serviceContextSupplier.get().getSchemaRegistryClientFactory();
  }

  @Override
  public ConnectClient getConnectClient() {
    return serviceContextSupplier.get().getConnectClient();
  }

  @Override
  public SimpleKaypherClient getKaypherClient() {
    return serviceContextSupplier.get().getKaypherClient();
  }

  @Override
  public void close() {
    serviceContextSupplier.get().close();
  }
}
