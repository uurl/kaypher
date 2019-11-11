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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Map;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;

/**
 * Configurable Schema Registry client factory, enabling SSL.
 */
public class KaypherSchemaRegistryClientFactory {

  private final SslFactory sslFactory;
  private final Supplier<RestService> serviceSupplier;
  private final Map<String, Object> schemaRegistryClientConfigs;
  private final SchemaRegistryClientFactory schemaRegistryClientFactory;
  private final Map<String, String> httpHeaders;

  interface SchemaRegistryClientFactory {
    CachedSchemaRegistryClient create(RestService service,
                                      int identityMapCapacity,
                                      Map<String, Object> clientConfigs,
                                      Map<String, String> httpHeaders);
  }


  public KaypherSchemaRegistryClientFactory(
      final KaypherConfig config,
      final Map<String, String> schemaRegistryHttpHeaders
  ) {
    this(config,
        () -> new RestService(config.getString(KaypherConfig.SCHEMA_REGISTRY_URL_PROPERTY)),
        new SslFactory(Mode.CLIENT),
        CachedSchemaRegistryClient::new,
        schemaRegistryHttpHeaders
    );

    // Force config exception now:
    config.getString(KaypherConfig.SCHEMA_REGISTRY_URL_PROPERTY);
  }

  KaypherSchemaRegistryClientFactory(final KaypherConfig config,
                                  final Supplier<RestService> serviceSupplier,
                                  final SslFactory sslFactory,
                                  final SchemaRegistryClientFactory schemaRegistryClientFactory,
                                  final Map<String, String> httpHeaders) {
    this.sslFactory = sslFactory;
    this.serviceSupplier = serviceSupplier;
    this.schemaRegistryClientConfigs = config.originalsWithPrefix(
        KaypherConfig.KAYPHER_SCHEMA_REGISTRY_PREFIX);

    this.sslFactory
        .configure(config.valuesWithPrefixOverride(KaypherConfig.KAYPHER_SCHEMA_REGISTRY_PREFIX));

    this.schemaRegistryClientFactory = schemaRegistryClientFactory;
    this.httpHeaders = httpHeaders;
  }

  public SchemaRegistryClient get() {
    final RestService restService = serviceSupplier.get();
    final SSLContext sslContext = sslFactory.sslEngineBuilder().sslContext();
    if (sslContext != null) {
      restService.setSslSocketFactory(sslContext.getSocketFactory());
    }

    return schemaRegistryClientFactory.create(
        restService,
        1000,
        schemaRegistryClientConfigs,
        httpHeaders
    );
  }
}
