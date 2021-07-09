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
package com.github.zabetak.calcite.tutorial;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.parser.SqlParseException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Common entry point for running queries over Apache Lucene from txt files by choosing the desired
 * query processor.
 *
 * The following query processors are available:
 * <ul>
 *   <li>{@link LuceneSimpleProcessor}</li>
 *   <li>{@link LuceneAdvancedProcessor}</li>
 *   <li>{@link LucenePushdownProcessor}</li>
 * </ul>
 */
public class LuceneExampleRunner {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: runner [SIMPLE|ADVANCED] SQL_FILE");
      System.exit(-1);
    }
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[1])), StandardCharsets.UTF_8);
    System.out.println("[Results]");
    long start = System.currentTimeMillis();
    for (Object row : runQuery(args[0], sqlQuery)) {
      if (row instanceof Object[]) {
        System.out.println(Arrays.toString((Object[]) row));
      } else {
        System.out.println(row);
      }
    }
    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  public static Enumerable<?> runQuery(String processorType, String query)
      throws SqlParseException {
    if (processorType.equalsIgnoreCase("SIMPLE")) {
      return LuceneSimpleProcessor.execute(query);
    } else if (processorType.equalsIgnoreCase("ADVANCED")) {
      return LuceneAdvancedProcessor.execute(query);
    } else if (processorType.equalsIgnoreCase("PUSHDOWN")) {
      return LucenePushdownProcessor.execute(query);
    } else {
      throw new IllegalArgumentException("Unsupported processor type: " + processorType);
    }
  }
}
