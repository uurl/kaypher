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

package com.koneksys.kaypher.metastore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.koneksys.kaypher.function.TestFunctionRegistry;
import com.koneksys.kaypher.metastore.model.kaypherStream;
import com.koneksys.kaypher.metastore.model.kaypherTable;
import com.koneksys.kaypher.metastore.model.kaypherTopic;
import com.koneksys.kaypher.metastore.model.StructuredDataSource;
import com.koneksys.kaypher.serde.DataSource;
import com.koneksys.kaypher.serde.DataSource.DataSourceType;
import com.koneksys.kaypher.serde.json.kaypherJsonTopicSerDe;
import com.koneksys.kaypher.util.MetaStoreFixture;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetastoreTest {

  private MutableMetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }

  @Test
  public void testTopicMap() {
    final kaypherTopic kaypherTopic1 = new kaypherTopic("testTopic", "testTopicKafka", new kaypherJsonTopicSerDe(), false);
    metaStore.putTopic(kaypherTopic1);
    final kaypherTopic kaypherTopic2 = metaStore.getTopic("testTopic");
    Assert.assertNotNull(kaypherTopic2);

    // Check non-existent topic
    final kaypherTopic kaypherTopic3 = metaStore.getTopic("TESTTOPIC_");
    Assert.assertNull(kaypherTopic3);
  }

  @Test
  public void testStreamMap() {
    final StructuredDataSource<?> structuredDataSource1 = metaStore.getSource("ORDERS");
    Assert.assertNotNull(structuredDataSource1);
    assertThat(structuredDataSource1.getDataSourceType(), is(DataSource.DataSourceType.KSTREAM));

    // Check non-existent stream
    final StructuredDataSource<?> structuredDataSource2 = metaStore.getSource("nonExistentStream");
    Assert.assertNull(structuredDataSource2);
  }

  @Test
  public void testDelete() {
    final StructuredDataSource<?> structuredDataSource1 = metaStore.getSource("ORDERS");
    final StructuredDataSource<?> structuredDataSource2 = new kaypherStream<>(
        "sqlexpression", "testStream",
        structuredDataSource1.getSchema(),
        structuredDataSource1.getKeyField(),
        structuredDataSource1.getTimestampExtractionPolicy(),
        structuredDataSource1.getkaypherTopic(),
        Serdes::String);

    metaStore.putSource(structuredDataSource2);
    final StructuredDataSource<?> structuredDataSource3 = metaStore.getSource("testStream");
    Assert.assertNotNull(structuredDataSource3);
    metaStore.deleteSource("testStream");
    final StructuredDataSource<?> structuredDataSource4 = metaStore.getSource("testStream");
    Assert.assertNull(structuredDataSource4);
  }

  @Test
  public void shouldGetTheCorrectSourceNameForTopic() {
    final StructuredDataSource<?> structuredDataSource = metaStore.getSourceForTopic("TEST2").get();
    assertThat(structuredDataSource, instanceOf(kaypherTable.class));
    assertThat(structuredDataSource.getDataSourceType(), equalTo(DataSourceType.KTABLE));
    assertThat(structuredDataSource.getName(), equalTo("TEST2"));
  }
}