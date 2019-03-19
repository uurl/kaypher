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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import com.koneksys.kaypher.serde.DataSource;
import com.koneksys.kaypher.serde.kaypherTopicSerDe;

@Immutable
public class KaypherTopic implements DataSource {

  private final String kaypherTopicName;
  private final String kafkaTopicName;
  private final kaypherTopicSerDe kaypherTopicSerDe;
  private final boolean iskaypherSink;

  public kaypherTopic(
      final String kaypherTopicName,
      final String kafkaTopicName,
      final kaypherTopicSerDe kaypherTopicSerDe,
      final boolean iskaypherSink
  ) {
    this.kaypherTopicName = requireNonNull(kaypherTopicName, "kaypherTopicName");
    this.kafkaTopicName = requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.kaypherTopicSerDe = requireNonNull(kaypherTopicSerDe, "kaypherTopicSerDe");
    this.iskaypherSink = iskaypherSink;
  }

  public kaypherTopicSerDe getkaypherTopicSerDe() {
    return kaypherTopicSerDe;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public String getkaypherTopicName() {
    return kaypherTopicName;
  }

  public boolean iskaypherSink() {
    return iskaypherSink;
  }

  @Override
  public String getName() {
    return kaypherTopicName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.KTOPIC;
  }
}
