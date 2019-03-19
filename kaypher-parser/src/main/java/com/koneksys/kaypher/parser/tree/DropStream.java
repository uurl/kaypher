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

package com.koneksys.kaypher.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.errorprone.annotations.Immutable;
import java.util.Optional;

@Immutable
public class DropStream extends AbstractStreamDropStatement implements ExecutableDdlStatement {

  public DropStream(
      final QualifiedName streamName,
      final boolean ifExists,
      final boolean deleteTopic
  ) {
    this(Optional.empty(), streamName, ifExists, deleteTopic);
  }

  public DropStream(
      final Optional<NodeLocation> location,
      final QualifiedName streamName,
      final boolean ifExists,
      final boolean deleteTopic
  ) {
    super(location, streamName, ifExists, deleteTopic);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropStream(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    return super.equals(obj);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("streamName", getName())
        .add("ifExists", getIfExists())
        .add("deleteTopic", isDeleteTopic())
        .toString();
  }
}
