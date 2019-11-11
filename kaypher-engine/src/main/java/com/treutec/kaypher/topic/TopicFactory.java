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
package com.treutec.kaypher.topic;

import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.model.WindowType;
import com.treutec.kaypher.parser.properties.with.CreateSourceProperties;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;

public final class TopicFactory {

  private TopicFactory() {
  }

  public static KaypherTopic create(final CreateSourceProperties properties) {
    final String kafkaTopicName = properties.getKafkaTopic();

    final Optional<WindowType> windowType = properties.getWindowType();
    final Optional<Duration> windowSize = properties.getWindowSize();

    final KeyFormat keyFormat = windowType
        .map(type -> KeyFormat
            .windowed(FormatInfo.of(Format.KAFKA), WindowInfo.of(type, windowSize)))
        .orElseGet(() -> KeyFormat
            .nonWindowed(FormatInfo.of(Format.KAFKA)));

    final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(
        properties.getValueFormat(),
        properties.getValueAvroSchemaName(),
        properties.getValueDelimiter()
    ));

    return new KaypherTopic(
        kafkaTopicName,
        keyFormat,
        valueFormat,
        false
    );
  }
}
