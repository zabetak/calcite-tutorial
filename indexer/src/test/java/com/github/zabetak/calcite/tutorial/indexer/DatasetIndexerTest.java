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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer.INDEX_LOCATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link DatasetIndexer}.
 */
public class DatasetIndexerTest {

  @BeforeAll
  static void indexTpchDataset() throws IOException, URISyntaxException {
    DatasetIndexer.main(new String[]{});
  }

  @Test
  void testTpchDatasetRowCounts() throws IOException {
    Map<TpchTable, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put(TpchTable.CUSTOMER, 150);
    expectedCounts.put(TpchTable.LINEITEM, 6005);
    expectedCounts.put(TpchTable.NATION, 25);
    expectedCounts.put(TpchTable.ORDERS, 1500);
    expectedCounts.put(TpchTable.PARTSUPP, 800);
    expectedCounts.put(TpchTable.PART, 200);
    expectedCounts.put(TpchTable.REGION, 5);
    expectedCounts.put(TpchTable.SUPPLIER, 10);
    for (TpchTable table : TpchTable.values()) {
      IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(INDEX_LOCATION, "tpch", table.name())));
      IndexSearcher searcher = new IndexSearcher(reader);
      assertEquals(expectedCounts.get(table), searcher.count(new MatchAllDocsQuery()), table.name());
    }
  }

  @Test
  void testTpchDatasetExtractData() throws IOException {
    for (TpchTable table : TpchTable.values()) {
      IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(INDEX_LOCATION, "tpch", table.name())));
      IndexSearcher searcher = new IndexSearcher(reader);
      for (ScoreDoc d : searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE).scoreDocs) {
        for (TpchTable.Column c : table.columns) {
          IndexableField field = reader.document(d.doc).getField(c.name);
          if (field != null) {
            if (Number.class.isAssignableFrom(c.type)) {
              assertNotNull(field.numericValue());
            } else {
              assertNotNull(field.stringValue());
            }
          }
        }
      }
    }
  }
}
