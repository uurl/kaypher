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
package com.treutec.kaypher.planner.plan;

import static com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext.Stacker;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.structured.SchemaKStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class FilterNodeTest {
  private final PlanNodeId nodeId = new PlanNodeId("nodeid");

  @Mock
  private Expression predicate;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream schemaKStream;
  @Mock
  private KaypherQueryBuilder kaypherStreamBuilder;
  @Mock
  private Stacker stacker;

  private FilterNode node;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(sourceNode.buildStream(any()))
        .thenReturn(schemaKStream);
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(schemaKStream.filter(any(), any()))
        .thenReturn(schemaKStream);

    when(kaypherStreamBuilder.buildNodeContext(nodeId.toString())).thenReturn(stacker);

    node = new FilterNode(nodeId, sourceNode, predicate);
  }

  @Test
  public void shouldApplyFilterCorrectly() {
    // When:
    node.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceNode).buildStream(kaypherStreamBuilder);
    verify(schemaKStream).filter(predicate, stacker);
  }
}
