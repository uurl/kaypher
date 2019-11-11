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
package com.treutec.kaypher.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import com.treutec.kaypher.execution.ddl.commands.DdlCommand;
import com.treutec.kaypher.execution.ddl.commands.DropSourceCommand;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.metastore.model.KaypherTable;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.tree.DropStream;
import com.treutec.kaypher.parser.tree.DropTable;
import com.treutec.kaypher.util.KaypherException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropSourceFactoryTest {
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");

  @Mock
  private KaypherStream kaypherStream;
  @Mock
  private KaypherTable kaypherTable;
  @Mock
  private MetaStore metaStore;

  private DropSourceFactory dropSourceFactory;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    when(metaStore.getSource(SOME_NAME)).thenReturn(kaypherStream);
    when(metaStore.getSource(TABLE_NAME)).thenReturn(kaypherTable);
    when(kaypherStream.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(kaypherTable.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    dropSourceFactory = new DropSourceFactory(metaStore);
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    // Given:
    final DropStream ddlStatement = new DropStream(SOME_NAME, true, true);

    // When:
    final DdlCommand result = dropSourceFactory.create(ddlStatement);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    // Given:
    final DropTable ddlStatement = new DropTable(TABLE_NAME, true, true);

    // When:
    final DdlCommand result = dropSourceFactory.create(ddlStatement);

    // Then:
    assertThat(result, instanceOf(DropSourceCommand.class));
  }

  @Test
  public void shouldCreateDropSourceOnMissingSourceWithIfExistsForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // When:
    final DropSourceCommand cmd = dropSourceFactory.create(dropStream);

    // Then:
    assertThat(cmd.getSourceName(), equalTo(SourceName.of("bob")));
  }

  @Test
  public void shouldFailDropSourceOnMissingSourceWithNoIfExistsForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, true, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(null);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Source bob does not exist.");

    // When:
    dropSourceFactory.create(dropStream);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFailDropSourceOnDropIncompatibleSourceForStream() {
    // Given:
    final DropStream dropStream = new DropStream(SOME_NAME, false, true);
    when(metaStore.getSource(SOME_NAME)).thenReturn(kaypherTable);

    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Incompatible data source type is TABLE");

    // When:
    dropSourceFactory.create(dropStream);
  }
}
