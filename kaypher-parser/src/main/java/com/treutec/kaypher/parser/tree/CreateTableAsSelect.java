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
package com.treutec.kaypher.parser.tree;

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.parser.properties.with.CreateSourceAsProperties;
import java.util.Optional;

@Immutable
public class CreateTableAsSelect extends CreateAsSelect {

  public CreateTableAsSelect(
      final SourceName name,
      final Query query,
      final boolean notExists,
      final CreateSourceAsProperties properties
  ) {
    this(Optional.empty(), name, query, notExists, properties);
  }

  public CreateTableAsSelect(
      final Optional<NodeLocation> location,
      final SourceName name,
      final Query query,
      final boolean notExists,
      final CreateSourceAsProperties properties
  ) {
    super(location, name, query, notExists, properties, Optional.empty());
  }

  private CreateTableAsSelect(
      final CreateTableAsSelect other,
      final CreateSourceAsProperties properties
  ) {
    super(other, properties);
  }

  @Override
  public CreateAsSelect copyWith(final CreateSourceAsProperties properties) {
    return new CreateTableAsSelect(this, properties);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTableAsSelect(this, context);
  }

  @Override
  public Sink getSink() {
    return Sink.of(getName(), true, getProperties(), Optional.empty());
  }

  @Override
  public String toString() {
    return "CreateTableAsSelect{" + super.toString() + '}';
  }
}
