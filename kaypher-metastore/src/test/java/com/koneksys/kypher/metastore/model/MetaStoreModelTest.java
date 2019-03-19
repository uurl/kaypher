/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.metastore.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import com.koneksys.kaypher.schema.kaypher.kaypherSchema;
import com.koneksys.kaypher.serde.kaypherTopicSerDe;
import com.koneksys.kaypher.serde.json.kaypherJsonTopicSerDe;
import com.koneksys.kaypher.test.util.ClassFinder;
import com.koneksys.kaypher.test.util.ImmutableTester;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Meta test to ensure all model classes meet certain requirements
 */
@RunWith(Parameterized.class)
public class MetaStoreModelTest {

  private static final ImmutableMap<Class<?>, Object> DEFAULTS = ImmutableMap
      .<Class<?>, Object>builder()
      .put(kaypherTopicSerDe.class, new kaypherJsonTopicSerDe())
      .put(kaypherTopic.class,
          new kaypherTopic("bob", "bob", new kaypherJsonTopicSerDe(), false))
      .put(org.apache.kafka.connect.data.Field.class,
          new org.apache.kafka.connect.data.Field("bob", 1, Schema.OPTIONAL_STRING_SCHEMA))
      .build();

  private final Class<?> modelClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ClassFinder.getClasses(StructuredDataSource.class.getPackage().getName());
  }

  public MetaStoreModelTest(final Class<?> modelClass) {
    this.modelClass = modelClass;
  }

  @Test
  public void shouldBeImmutable() {
    new ImmutableTester()
        .withKnownImmutableType(kaypherSchema.class)
        .withKnownImmutableType(org.apache.kafka.connect.data.Field.class)
        .test(modelClass);
  }

  @Test
  public void shouldThrowNpeFromConstructors() {
    assumeThat(Modifier.isAbstract(modelClass.getModifiers()), is(false));

    getNullPointerTester()
        .testConstructors(modelClass, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowNpeFromFactoryMethods() {
    getNullPointerTester()
        .testStaticMethods(modelClass, Visibility.PACKAGE);
  }

  @SuppressWarnings({"unchecked", "UnstableApiUsage"})
  private static NullPointerTester getNullPointerTester() {
    final NullPointerTester tester = new NullPointerTester();
    DEFAULTS.forEach((type, value) -> {
      assertThat(value, is(instanceOf(type)));
      tester.setDefault((Class) type, value);
    });
    return tester;
  }
}