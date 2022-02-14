/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial.indexer;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A reader for TPCH CSV files located in a predefined location in the classpath.
 */
class TpchCSVReader {
  private static final char DELIMITER = '|';

  private final TpchTable table;

  public static TpchCSVReader of(TpchTable table) {
    return new TpchCSVReader(table);
  }

  private TpchCSVReader(TpchTable table) {
    this.table = table;
  }

  public void forEach(Consumer<String[]> action) throws IOException {
    Objects.requireNonNull(action);
    String path = Paths.get("data", "tpch", table.name().toLowerCase().concat(".csv")).toString();
    try (CSVReader reader = new CSVReader(getResourceAsReader(path), DELIMITER)) {
      String[] values = reader.readNext();
      while (values != null) {
        action.accept(values);
        values = reader.readNext();
      }
    }
  }

  private static Reader getResourceAsReader(String path) {
    return new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(path));
  }
}
