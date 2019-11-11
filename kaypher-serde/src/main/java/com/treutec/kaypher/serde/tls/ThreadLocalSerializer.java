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

package com.treutec.kaypher.serde.tls;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serializer;

public class ThreadLocalSerializer<T> implements Serializer<T> {

  private final ThreadLocalCloseable<Serializer<T>> serializer;

  public ThreadLocalSerializer(final Supplier<Serializer<T>> initialValueSupplier) {
    serializer = new ThreadLocalCloseable<>(initialValueSupplier);
  }

  @Override
  public void configure(final Map<String, ?> properties, final boolean isKey) {
    serializer.get().configure(properties, isKey);
  }

  @Override
  public byte[] serialize(final String topicName, final T record) {
    return serializer.get().serialize(topicName, record);
  }

  @Override
  public void close() {
    serializer.close();
  }
}
