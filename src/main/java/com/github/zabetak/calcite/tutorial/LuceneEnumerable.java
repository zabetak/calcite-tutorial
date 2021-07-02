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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A class providing enumerators over an Apache Lucene index.
 */
public class LuceneEnumerable extends AbstractEnumerable<Object[]> {
  private final String indexPath;
  private final Map<String, SqlTypeName> fields;
  private final String query;

  public LuceneEnumerable(String indexPath, Map<String, SqlTypeName> fields, String query) {
    this.indexPath = indexPath;
    this.fields = fields;
    this.query = query;
  }

  @Override public Enumerator<Object[]> enumerator() {
    return Linq4j.enumerator(searchIndex());
  }

  private List<Object[]> searchIndex() {
    Query q;
    try {
      q = new StandardQueryParser().parse(query,"");
    } catch (QueryNodeException e) {
      throw new RuntimeException(e);
    }
    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = new IndexSearcher(reader);
      List<Object[]> result = new ArrayList<>();
      for (ScoreDoc d : searcher.search(q, Integer.MAX_VALUE).scoreDocs) {
        Object[] row = new Object[fields.size()];
        int i = 0;
        for (Map.Entry<String, SqlTypeName> field : fields.entrySet()) {
          IndexableField indexField = reader.document(d.doc).getField(field.getKey());
          row[i++] = extractValueForType(indexField, field.getValue());
        }
        result.add(row);
      }
      return result;
    } catch (IOException exception) {
      // If the index is not found or for some reason we cannot read it consider the table empty
      return Collections.emptyList();
    }
  }

  private static Object extractValueForType(IndexableField field, SqlTypeName typeName) {
    if (field != null) {
      switch (typeName) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case FLOAT:
      case DECIMAL:
        return field.numericValue();
      case DATE:
        return Date.valueOf(LocalDate.ofEpochDay((int) field.numericValue()));
      case VARCHAR:
      case CHAR:
        return field.stringValue();
      default:
        throw new IllegalStateException();
      }
    }
    return null;
  }
}
