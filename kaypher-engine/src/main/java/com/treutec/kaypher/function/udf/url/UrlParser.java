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
package com.treutec.kaypher.function.udf.url;

import com.treutec.kaypher.function.KaypherFunctionException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Utility class for extracting information from a String encoded URL
 */
final class UrlParser {

  private UrlParser() { }

  /**
   * @param url       an application/x-www-form-urlencoded string
   * @param extract   a method that accepts a {@link URI} and returns the value to extract
   *
   * @return the value of {@code extract(url)} if present and valid, otherwise {@code null}
   */
  static <T> T extract(final String url, @Nonnull final Function<URI, T> extract) {
    Objects.requireNonNull(extract, "must supply a non-null extract method!");

    if (url == null) {
      return null;
    }

    try {
      return extract.apply(new URI(url));
    } catch (final URISyntaxException e) {
      throw new KaypherFunctionException("URL input has invalid syntax: " + url, e);
    }
  }
}
