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

package com.treutec.kaypher.connect;

import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import org.junit.Test;

public class ConnectorTest {

  @Test
  public void shouldImplementHashAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new Connector("foo", foo -> true, foo -> foo, DataSourceType.KTABLE, "key"),
            new Connector("foo", foo -> false, foo -> foo, DataSourceType.KTABLE, "key"),
            new Connector("foo", foo -> false, foo -> foo, DataSourceType.KTABLE, "key")
        ).addEqualityGroup(
            new Connector("bar", foo -> true, foo -> foo, DataSourceType.KTABLE, "key")
        ).addEqualityGroup(
            new Connector("foo", foo -> true, foo -> foo, DataSourceType.KTABLE, "key2")
        ).addEqualityGroup(
            new Connector("foo", foo -> true, foo -> foo, DataSourceType.KSTREAM, "key")
        )
        .testEquals();
  }

}