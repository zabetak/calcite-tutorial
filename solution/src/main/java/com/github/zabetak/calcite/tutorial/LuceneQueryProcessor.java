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
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;
import com.github.zabetak.calcite.tutorial.operators.LuceneRel;
import com.github.zabetak.calcite.tutorial.rules.LuceneFilterRule;
import com.github.zabetak.calcite.tutorial.rules.LuceneTableScanRule;
import com.github.zabetak.calcite.tutorial.rules.LuceneToEnumerableConverterRule;

import javax.sql.DataSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Query processor for running TPC-H queries over Apache Lucene.
 */
public class LuceneQueryProcessor {

  /**
   * The type of query processor.
   */
  public enum Type {
    /**
     * Simple query processor using only one convention.
     *
     * The processor relies on {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
     * and the {@link org.apache.calcite.schema.ScannableTable} interface to execute queries over
     * Lucene.
     */
    SIMPLE,
    /**
     * Advanced query processor using two conventions.
     *
     * The processor relies on {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
     * and {@link LuceneRel#LUCENE} to execute queries over Lucene.
     */
    ADVANCED,
    /**
     * Advanced query processor using two conventions and extra rules capable of pushing basic
     * conditions in Lucene.
     *
     * The processor relies on {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
     * and {@link LuceneRel#LUCENE} to execute queries over Lucene.
     */
    PUSHDOWN,
    /**
     * Query processor for JDBC datasources.
     *
     * The processor relies on {@link org.apache.calcite.adapter.jdbc.JdbcConvention} and
     * {@link EnumerableConvention} to execute queries over JDBC datasources.
     */
    JDBC
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: runner [SIMPLE|ADVANCED|PUSHDOWN|JDBC] SQL_FILE");
      System.exit(-1);
    }
    Type pType = Type.valueOf(args[0]);
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[1])), StandardCharsets.UTF_8);
    System.out.println("[Results]");
    long start = System.currentTimeMillis();
    for (Object row : execute(sqlQuery, pType)) {
      if (row instanceof Object[]) {
        System.out.println(Arrays.toString((Object[]) row));
      } else {
        System.out.println(row);
      }
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
  public static <T> Enumerable<T> execute(String sqlQuery, Type processorType)
      throws SqlParseException {
    System.out.println("[Input query]");
    System.out.println(sqlQuery);

    // Create the schema and table data types
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    Map<String, Table> luceneTables = new HashMap<>();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    for (TpchTable table : TpchTable.values()) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      for (TpchTable.Column column : table.columns) {
        RelDataType type = typeFactory.createJavaType(column.type);
        builder.add(column.name, type.getSqlTypeName()).nullable(true);
      }
      String indexPath = DatasetIndexer.INDEX_LOCATION + "/tpch/" + table.name();
      luceneTables.put(table.name(), new LuceneTable(indexPath, builder.build()));
    }
    rootSchema.add("lucene", new AbstractSchema(){
      @Override protected Map<String, Table> getTableMap() {
        return luceneTables;
      }
    });
    DataSource dataSource =
        JdbcSchema.dataSource("jdbc:hsqldb:res:foodmart", "org.hsqldb.jdbc.JDBCDriver", "sa", "");
    // It was a bit tricky to find the understand why a schema parameter was needed
    // It is also a bit counterintuitive the fact that we need to pass the rootSchema.plus; it is
    // necessary cause we want to create an expression towards the parent to use with enumerable
    // operators. The name we e.g., 'fm' must match the name we add the jdbc to root.
    JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema.plus(),"fm", dataSource,null,"foodmart");
    rootSchema.add("fm", jdbcSchema);
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
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema,
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
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_MINUS_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_MATCH_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
    switch (processorType) {
    case PUSHDOWN:
      planner.addRule(LuceneFilterRule.DEFAULT.toRule());
      // Fall-through
    case ADVANCED:
      planner.addRule(LuceneTableScanRule.DEFAULT.toRule());
      planner.addRule(LuceneToEnumerableConverterRule.DEFAULT.toRule());
      break;
    case SIMPLE:
      planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
      break;
    case JDBC:
      break;
    default:
      throw new AssertionError();
    }

    // Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logPlan);
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

    // Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Obtain the executable plan
    Bindable<T> executablePlan = EnumerableInterpretable.toBindable(
        new HashMap<>(),
        null,
        phyPlan,
        EnumerableRel.Prefer.ARRAY);
    // Run the executable plan using a context simply providing access to the schema
    return executablePlan.bind(new SchemaOnlyDataContext(rootSchema));
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
