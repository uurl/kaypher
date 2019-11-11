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

import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.DefaultTraversalVisitor;
import com.treutec.kaypher.parser.tree.AliasedRelation;
import com.treutec.kaypher.parser.tree.AstNode;
import com.treutec.kaypher.parser.tree.Join;
import com.treutec.kaypher.parser.tree.Table;
import com.treutec.kaypher.util.KaypherException;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class that extracts all source topics from a query node.
 */
public class SourceTopicsExtractor extends DefaultTraversalVisitor<AstNode, Void> {
  private final Set<String> sourceTopics = new HashSet<>();
  private final MetaStore metaStore;

  private String primaryKafkaTopicName = null;

  public SourceTopicsExtractor(final MetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public String getPrimaryKafkaTopicName() {
    return primaryKafkaTopicName;
  }

  public Set<String> getSourceTopics() {
    return sourceTopics;
  }

  @Override
  protected AstNode visitJoin(final Join node, final Void context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  protected AstNode visitAliasedRelation(final AliasedRelation node, final Void context) {
    final SourceName structuredDataSourceName = ((Table) node.getRelation()).getName();
    final DataSource<?> source = metaStore.getSource(structuredDataSourceName);
    if (source == null) {
      throw new KaypherException(structuredDataSourceName.name() + " does not exist.");
    }

    // This method is called first with the primary kafka topic (or the node.getFrom() node)
    if (primaryKafkaTopicName == null) {
      primaryKafkaTopicName = source.getKafkaTopicName();
    }

    sourceTopics.add(source.getKafkaTopicName());
    return node;
  }
}
