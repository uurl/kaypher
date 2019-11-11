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

package com.treutec.kaypher.statement;

import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.schema.kaypher.inference.DefaultSchemaInjector;
import com.treutec.kaypher.schema.kaypher.inference.SchemaRegistryTopicSchemaSupplier;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.topic.TopicCreateInjector;
import com.treutec.kaypher.topic.TopicDeleteInjector;
import java.util.Objects;
import java.util.function.BiFunction;

public enum Injectors implements BiFunction<KaypherExecutionContext, ServiceContext, Injector> {

  NO_TOPIC_DELETE((ec, sc) -> InjectorChain.of(
      new DefaultSchemaInjector(
          new SchemaRegistryTopicSchemaSupplier(sc.getSchemaRegistryClient())),
      new TopicCreateInjector(ec, sc)
  )),

  DEFAULT((ec, sc) -> InjectorChain.of(
      NO_TOPIC_DELETE.apply(ec, sc),
      new TopicDeleteInjector(ec, sc)
  ))
  ;

  private final BiFunction<KaypherExecutionContext, ServiceContext, Injector> injectorFactory;

  Injectors(final BiFunction<KaypherExecutionContext, ServiceContext, Injector> injectorFactory) {
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
  }

  @Override
  public Injector apply(final KaypherExecutionContext executionContext,
      final ServiceContext serviceContext) {
    return injectorFactory.apply(executionContext, serviceContext);
  }
}