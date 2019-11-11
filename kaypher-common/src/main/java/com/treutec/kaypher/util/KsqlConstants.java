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

public final class KaypherConstants {

  private KaypherConstants() {
  }

  public static final String TREUTEC_AUTHOR = "Treutec";

  public static final String KAYPHER_INTERNAL_TOPIC_PREFIX = "_treutec-kaypher-";
  public static final String CONFLUENT_INTERNAL_TOPIC_PREFIX = "__treutec";

  public static final String STREAMS_CHANGELOG_TOPIC_SUFFIX = "-changelog";
  public static final String STREAMS_REPARTITION_TOPIC_SUFFIX = "-repartition";

  public static final String SCHEMA_REGISTRY_VALUE_SUFFIX = "-value";

  public static final int legacyDefaultSinkPartitionCount = 4;
  public static final short legacyDefaultSinkReplicaCount = 1;
  public static final long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public static final String defaultAutoOffsetRestConfig = "latest";
  public static final long defaultCommitIntervalMsConfig = 2000;
  public static final long defaultCacheMaxBytesBufferingConfig = 10000000;
  public static final int defaultNumberOfStreamsThreads = 4;

  public static final String LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT = "kaypher.run.script.statements";

  public static final String DOT = ".";
  public static final String ESCAPE = "`";
  public static final String STRUCT_FIELD_REF = "->";

  public static final String AVRO_SCHEMA_NAMESPACE = "com.treutec.kaypher.avro_schemas";
  public static final String AVRO_SCHEMA_NAME = "KaypherDataSourceSchema";
  public static final String DEFAULT_AVRO_SCHEMA_FULL_NAME =
          AVRO_SCHEMA_NAMESPACE + "." + AVRO_SCHEMA_NAME;

  /**
   * Default time and date patterns
   */
  public static final String TIME_PATTERN = "HH:mm:ss.SSS";
  public static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'" + TIME_PATTERN;
}
