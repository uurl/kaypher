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

package com.treutec.kaypher.properties.with;

import org.apache.kafka.common.config.ConfigDef;

/**
 * 'With Clause' properties for 'CREATE AS' statements.
 */
public final class CreateAsConfigs {

  private static final ConfigDef CONFIG_DEF = new ConfigDef();

  static {
    CommonCreateConfigs.addToConfigDef(CONFIG_DEF, false, false);
  }

  public static final ConfigMetaData CONFIG_METADATA = ConfigMetaData.of(CONFIG_DEF);

  private CreateAsConfigs() {
  }
}
