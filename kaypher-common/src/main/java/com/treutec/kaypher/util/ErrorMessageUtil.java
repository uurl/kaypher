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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.text.WordUtils;

public final class ErrorMessageUtil {

  private static final String PREFIX = "Caused by: ";

  private ErrorMessageUtil() {
  }

  /**
   * Build an error message containing the message of each throwable in the chain.
   *
   * <p>Throwable messages are separated by new lines.
   *
   * @param throwable the top level error.
   * @return the error message.
   */
  public static String buildErrorMessage(final Throwable throwable) {
    if (throwable == null) {
      return "";
    }

    final List<String> messages = getErrorMessages(throwable);
    dedup(messages);

    final String msg = messages.remove(0);

    final String causeMsg = messages.stream()
        .filter(s -> !s.isEmpty())
        .map(cause -> WordUtils.wrap(PREFIX + cause, 80, "\n\t", true))
        .collect(Collectors.joining(System.lineSeparator()));

    return causeMsg.isEmpty() ? msg : msg + System.lineSeparator() + causeMsg;
  }

  /**
   * Build a list containing the error message for each throwable in the chain.
   *
   * @param e the top level error.
   * @return the list of error messages.
   */
  public static List<String> getErrorMessages(final Throwable e) {
    return getThrowables(e).stream()
        .map(ErrorMessageUtil::getErrorMessage)
        .collect(Collectors.toList());
  }

  private static String getErrorMessage(final Throwable e) {
    if (e instanceof ConnectException) {
      return "Could not connect to the server.";
    } else {
      return e.getMessage() == null ? e.toString() : e.getMessage();
    }
  }

  private static List<Throwable> getThrowables(final Throwable e) {
    final List<Throwable> list = new ArrayList<>();
    Throwable cause = e;
    while (cause != null && !list.contains(cause)) {
      list.add(cause);
      cause = cause.getCause();
    }
    return list;
  }

  private static void dedup(final List<String> messages) {
    while (messages.size() > 1) {
      if (!messages.get(0).equals(messages.get(1))) {
        return;
      }

      messages.remove(0);
    }
  }
}
