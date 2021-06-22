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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import com.github.zabetak.calcite.tutorial.setup.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.setup.TpchTable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * An end to end example from an SQL query to a plan over Lucene indexes using Calcite's
 * {@link org.apache.calcite.interpreter.BindableConvention}.
 */
public class EndToEndExampleLuceneSimple {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Missing path to SQL input file");
      System.exit(-1);
    }
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);

    System.out.println("[Results]");
    long start = System.currentTimeMillis();
    for (Object[] row : execute(sqlQuery)) {
      System.out.println(Arrays.toString(row));
    }
    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  /**
   * Plans and executes an SQL query.
   *
   * @param sqlQuery - a string with the SQL query for execution
   * @return an Enumerable with the results of the execution of the query
   * @throws SqlParseException if there is a problem when parsing the query
   */
  public static Enumerable<Object[]> execute(String sqlQuery) throws SqlParseException {
    System.out.println("[Input query]");
    System.out.println(sqlQuery);

    // Create the schema and table data types
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    for (TpchTable table : TpchTable.values()) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      for (TpchTable.Column column : table.columns) {
        RelDataType type = typeFactory.createJavaType(column.type);
        builder.add(column.name, type).nullable(true);
      }
      String indexPath = DatasetIndexer.INDEX_LOCATION + "/tpch/" + table.name();
      schema.add(table.name(), new LuceneTable(indexPath, builder.build()));
    }

    // Create an SQL parser
    SqlParser parser = SqlParser.create(sqlQuery);
    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();
    System.out.println("[Parsed query]");
    System.out.println(sqlNode.toString());

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList("bs"),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    // Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    // Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    // Display the logical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
    planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
    planner.addRule(Bindables.BINDABLE_FILTER_RULE);
    planner.addRule(Bindables.BINDABLE_JOIN_RULE);
    planner.addRule(Bindables.BINDABLE_SORT_RULE);
    planner.addRule(Bindables.BINDABLE_AGGREGATE_RULE);
    planner.addRule(Bindables.BINDABLE_VALUES_RULE);
    planner.addRule(Bindables.BINDABLE_SET_OP_RULE);
    planner.addRule(Bindables.BINDABLE_MATCH_RULE);
    planner.addRule(Bindables.BINDABLE_WINDOW_RULE);

    // Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(BindableConvention.INSTANCE));
    planner.setRoot(logPlan);
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    BindableRel phyPlan = (BindableRel) planner.findBestExp();

    // Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Run the executable plan using a context simply providing access to the schema
    return phyPlan.bind(new SchemaOnlyDataContext(schema));
  }

  /**
   * Table representing an Apache Lucene index.
   *
   * The table implements the {@link ScannableTable} interface and knows how to extract rows
   * from Lucene and map them to Calcite's internal representation.
   */
  private static final class LuceneTable extends AbstractTable implements ScannableTable {
    private final String indexPath;
    private final RelDataType dataType;

    LuceneTable(String indexPath, RelDataType dataType) {
      this.indexPath = indexPath;
      this.dataType = dataType;
    }

    @Override public Enumerable<Object[]> scan(final DataContext root) {
      try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
        IndexSearcher searcher = new IndexSearcher(reader);
        List<Object[]> result = new ArrayList<>();
        for (ScoreDoc d : searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE).scoreDocs) {
          Object[] row = new Object[dataType.getFieldCount()];
          for (int i = 0; i < dataType.getFieldCount(); i++) {
            RelDataTypeField typeField = dataType.getFieldList().get(i);
            IndexableField indexField = reader.document(d.doc).getField(typeField.getName());
            row[i] = extractValueForType(indexField, typeField.getType().getSqlTypeName());
          }
          result.add(row);
        }
        return Linq4j.asEnumerable(result);
      } catch (IOException exception) {
        // If the index is not found or for some reason we cannot read it consider the table empty
        return Linq4j.emptyEnumerable();
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

    @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return typeFactory.copyType(dataType);
    }
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
