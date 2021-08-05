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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * A class for indexing predefined datasets in Apache Lucene.
 *
 * The following datasets are available:
 * <ul>
 *   <li>TPC-H with scale factor 0.001</li>
 * </ul>
 *
 * The indexer reads data from CSV files and creates a single Lucene index per file. The indexes
 * are created under the {@link #INDEX_LOCATION} directory and (with the current configuration)
 * are overwritten every time the indexer runs.
 */
public class DatasetIndexer {
  private static final char DELIMITER = '|';
  private static final String DATASET_LOCATION = "data";
  public static final String INDEX_LOCATION = "target";
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public static void main(String[] args) throws IOException, URISyntaxException {
    for (TpchTable table : TpchTable.values()) {
      indexTable("tpch", table);
    }
  }

  private static void indexTable(final String dataset, TpchTable table)
      throws IOException {
    final String tablePath =
        DATASET_LOCATION + "/" + dataset + "/" + table.name().toLowerCase().concat(".csv");
    try (Directory indexDir = FSDirectory.open(Paths.get(INDEX_LOCATION, dataset, table.name()))) {
      IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer());
      writerConfig.setOpenMode(OpenMode.CREATE);
      try (IndexWriter writer = new IndexWriter(indexDir, writerConfig)) {
        try (CSVReader reader = new CSVReader(getResourceAsReader(tablePath), DELIMITER)) {
          String[] values = reader.readNext();
          while (values != null) {
            Document doc = new Document();
            for (int i = 0; i < table.columns.size() && i < values.length; i++) {
              TpchTable.Column c = table.columns.get(i);
              indexValue(doc, c, values[i]);
            }
            writer.addDocument(doc);
            values = reader.readNext();
          }
        }
      }
    }
  }

  private static void indexValue(Document doc, TpchTable.Column column, String value) {
    if (value.equals("")) {
      return;
    }
    if (Integer.class == column.type) {
      int intVal = Integer.valueOf(value);
      doc.add(new StoredField(column.name, intVal));
      doc.add(new IntPoint(column.name, intVal));
    } else if (String.class == column.type) {
      doc.add(new StringField(column.name, value, Field.Store.YES));
    } else if (Double.class == column.type) {
      double dblVal = Double.valueOf(value);
      doc.add(new StoredField(column.name, dblVal));
      doc.add(new DoublePoint(column.name, dblVal));
    } else if (Date.class == column.type) {
      int epochDays = Math.toIntExact(LocalDate.parse(value, FORMATTER).toEpochDay());
      doc.add(new StoredField(column.name, epochDays));
      doc.add(new IntPoint(column.name, epochDays));
    } else {
      throw new IllegalStateException();
    }
  }

  private static Reader getResourceAsReader(String path) {
    return new InputStreamReader(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(path));
  }
}
