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

public class KaypherStatementException extends KaypherException {

  private final String sqlStatement;
  private final String rawMessage;

  public KaypherStatementException(final String message, final String sqlStatement) {
    super(buildMessage(message, sqlStatement));
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
  }

  public KaypherStatementException(
      final String message,
      final String sqlStatement,
      final Throwable cause) {
    super(buildMessage(message, sqlStatement), cause);
    this.rawMessage = message == null ? "" : message;
    this.sqlStatement = sqlStatement == null ? "" : sqlStatement;
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  public String getRawMessage() {
    return rawMessage;
  }

  private static String buildMessage(final String message, final String sqlStatement) {
    return message + System.lineSeparator() + "Statement: " + sqlStatement;
  }
}
