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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

/**
 * A class for loading TPCH dataset in Apache Lucene and HyperSQL.
 *
 * @see JDBCDatasetLoader
 * @see LuceneDatasetLoader
 */
public class DatasetLoader {
  public static final Path LUCENE_INDEX_PATH = Paths.get("target", "tpch", "lucene");
  public static final Path JDBC_HSQLDB_PATH = Paths.get("target", "tpch", "hsqldb", "tpchdb");

  public static void main(String[] args) throws IOException, SQLException {
    new LuceneDatasetLoader(LUCENE_INDEX_PATH).load();
    String url = "jdbc:hsqldb:file:" + JDBC_HSQLDB_PATH;
    new JDBCDatasetLoader(url, "SA", "").load();
  }
}
