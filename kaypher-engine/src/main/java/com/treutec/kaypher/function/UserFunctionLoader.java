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
package com.treutec.kaypher.function;

import static java.util.Optional.empty;

import com.treutec.kaypher.function.udaf.UdafDescription;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udtf.UdtfDescription;
import com.treutec.kaypher.metastore.TypeRegistry;
import com.treutec.kaypher.metrics.MetricCollectors;
import com.treutec.kaypher.schema.kaypher.SqlTypeParser;
import com.treutec.kaypher.security.ExtensionSecurityManager;
import com.treutec.kaypher.util.KaypherConfig;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates the loading of UDFs, UDAFs and UDTFs. The actual loading of the functions is done in
 * of the specific function loader classes
 */
public class UserFunctionLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserFunctionLoader.class);

  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final Predicate<String> blacklist;
  private final boolean loadCustomerUdfs;
  private final UdfLoader udfLoader;
  private final UdafLoader udafLoader;
  private final UdtfLoader udtfLoader;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public UserFunctionLoader(
      final MutableFunctionRegistry functionRegistry,
      final File pluginDir,
      final ClassLoader parentClassLoader,
      final Predicate<String> blacklist,
      final Optional<Metrics> metrics,
      final boolean loadCustomerUdfs
  ) {
    Objects.requireNonNull(functionRegistry, "functionRegistry can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(
        parentClassLoader,
        "parentClassLoader can't be null"
    );
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    Objects.requireNonNull(metrics, "metrics can't be null");
    this.loadCustomerUdfs = loadCustomerUdfs;
    final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
    this.udfLoader = new UdfLoader(functionRegistry, metrics, typeParser, false);
    this.udafLoader = new UdafLoader(functionRegistry, metrics, typeParser);
    this.udtfLoader = new UdtfLoader(functionRegistry, metrics, typeParser, false);
  }

  public void load() {
    // load functions packaged as part of kaypher first
    loadFunctions(parentClassLoader, empty());
    if (loadCustomerUdfs) {
      try {
        if (!pluginDir.exists() && !pluginDir.isDirectory()) {
          LOGGER.info(
              "UDFs can't be loaded as as dir {} doesn't exist or is not a directory",
              pluginDir
          );
          return;
        }
        Files.find(pluginDir.toPath(), 1,
            (path, attributes) -> path.toString().endsWith(".jar")
        )
            .map(path -> UdfClassLoader.newClassLoader(path, parentClassLoader, blacklist))
            .forEach(classLoader ->
                loadFunctions(classLoader, Optional.of(classLoader.getJarPath())));
      } catch (final IOException e) {
        LOGGER.error("Failed to load UDFs from location {}", pluginDir, e);
      }
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private void loadFunctions(final ClassLoader loader, final Optional<Path> path) {
    final String pathLoadedFrom
        = path.map(Path::toString).orElse(KaypherScalarFunction.INTERNAL_PATH);
    final FastClasspathScanner fastClasspathScanner = new FastClasspathScanner();
    if (loader != parentClassLoader) {
      fastClasspathScanner.overrideClassLoaders(loader);
    }
    fastClasspathScanner
        .ignoreParentClassLoaders()
        // if we are loading from the parent classloader then restrict the name space to only
        // jars/dirs containing "kaypher-engine". This is so we don't end up scanning every jar
        .filterClasspathElements(
            name -> {
              if (parentClassLoader != loader) {
                return true;
              }
              return name.contains("kaypher-engine");
            })
        .matchClassesWithAnnotation(
            UdfDescription.class,
            theClass -> udfLoader.loadUdfFromClass(theClass, pathLoadedFrom)
        )
        .matchClassesWithAnnotation(
            UdafDescription.class,
            theClass -> udafLoader.loadUdafFromClass(theClass, pathLoadedFrom)
        )
        .matchClassesWithAnnotation(
            UdtfDescription.class,
            theClass -> udtfLoader.loadUdtfFromClass(theClass, pathLoadedFrom)
        )
        .scan();
  }

  public static UserFunctionLoader newInstance(
      final KaypherConfig config,
      final MutableFunctionRegistry metaStore,
      final String kaypherInstallDir
  ) {
    final Boolean loadCustomerUdfs = config.getBoolean(KaypherConfig.KAYPHER_ENABLE_UDFS);
    final Boolean collectMetrics = config.getBoolean(KaypherConfig.KAYPHER_COLLECT_UDF_METRICS);
    final String extDirName = config.getString(KaypherConfig.KAYPHER_EXT_DIR);
    final File pluginDir = KaypherConfig.DEFAULT_EXT_DIR.equals(extDirName)
        ? new File(kaypherInstallDir, extDirName)
        : new File(extDirName);

    final Optional<Metrics> metrics = collectMetrics
        ? Optional.of(MetricCollectors.getMetrics())
        : empty();

    if (config.getBoolean(KaypherConfig.KAYPHER_UDF_SECURITY_MANAGER_ENABLED)) {
      System.setSecurityManager(ExtensionSecurityManager.INSTANCE);
    }
    return new UserFunctionLoader(
        metaStore,
        pluginDir,
        Thread.currentThread().getContextClassLoader(),
        new Blacklist(new File(pluginDir, "resource-blacklist.txt")),
        metrics,
        loadCustomerUdfs
    );
  }
}
