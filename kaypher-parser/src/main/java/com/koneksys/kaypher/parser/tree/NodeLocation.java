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

import com.google.errorprone.annotations.Immutable;

@Immutable
public final class NodeLocation {

  private final int line;
  private final int charPositionInLine;

  public NodeLocation(final int line, final int charPositionInLine) {
    this.line = line;
    this.charPositionInLine = charPositionInLine;
  }

  public int getLineNumber() {
    return line;
  }

  public int getColumnNumber() {
    return charPositionInLine + 1;
  }

  @Override
  public String toString() {
    return String.format("Line: %d, Col: %d", line, charPositionInLine + 1);
  }
}
