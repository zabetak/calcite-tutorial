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
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import com.github.zabetak.calcite.tutorial.setup.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.setup.TpchTable;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * An end to end example from an SQL query to a plan over Lucene indexes using Calcite's
 * {@link EnumerableConvention} and a custom convention.
 */
public class EndToEndExampleLuceneAdvanced {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Missing path to SQL input file");
      System.exit(-1);
    }
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);

    System.out.println("[Results]");
    long start = System.currentTimeMillis();
    for (Object row : EndToEndExampleLuceneAdvanced.execute(sqlQuery)) {
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
   * @param sqlQuery a string with the SQL query for execution
   * @return an Enumerable with the results of the execution of the query
   * @throws SqlParseException if there is a problem when parsing the query
   */
  public static <T> Enumerable<T> execute(String sqlQuery) throws SqlParseException {
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
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(LuceneTableScanRule.DEFAULT.toRule());
    planner.addRule(LuceneEnumerableConverterRule.DEFAULT.toRule());
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
    return executablePlan.bind(new SchemaOnlyDataContext(schema));
  }

  private static class LuceneTableScanRule extends ConverterRule {
    @Override public RelNode convert(final RelNode rel) {
      final LogicalTableScan scan = (LogicalTableScan) rel;
      final LuceneTable table = scan.getTable().unwrap(LuceneTable.class);
      if (table != null) {
        return
            new LuceneTableScan(
                scan.getCluster(),
                scan.getCluster().traitSetOf(LUCENE),
                Collections.emptyList(),
                scan.getTable());
      }
      return null;
    }

    public LuceneTableScanRule(final Config config) {
      super(config);
    }

    static final Config DEFAULT = Config.INSTANCE
        .withConversion(LogicalTableScan.class, Convention.NONE,
            LUCENE, "LuceneTableScanRule")
        .withRuleFactory(LuceneTableScanRule::new);
  }

  private static class LuceneEnumerableConverterRule extends ConverterRule {
    public LuceneEnumerableConverterRule(final Config config) {
      super(config);
    }

    @Override public RelNode convert(final RelNode rel) {
      return new LuceneEnumerableConverter(rel);
    }

    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(LuceneRel.class,
            LUCENE, EnumerableConvention.INSTANCE,
            "LuceneBindableConverterRule")
        .withRuleFactory(LuceneEnumerableConverterRule::new);
  }

  /**
   * Table representing an Apache Lucene index.
   *
   * The table implements the {@link ScannableTable} interface and knows how to extract rows
   * from Lucene and map them to Calcite's internal representation.
   */
  private static final class LuceneTable extends AbstractTable {
    private final String indexPath;
    private final RelDataType dataType;

    LuceneTable(String indexPath, RelDataType dataType) {
      this.indexPath = indexPath;
      this.dataType = dataType;
    }

    @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return typeFactory.copyType(dataType);
    }
  }

  private interface LuceneRel extends RelNode {
    Result implement();

    class Result {
      public final String indexPath;
      public final Query query;

      public Result(final String indexPath, final Query query) {
        this.indexPath = indexPath;
        this.query = query;
      }
    }
  }

  private static final Convention LUCENE = new Convention.Impl("LUCENE", LuceneRel.class);

  private static class LuceneTableScan extends TableScan implements LuceneRel {
    protected LuceneTableScan(final RelOptCluster cluster, final RelTraitSet traitSet,
        final List<RelHint> hints, final RelOptTable table) {
      super(cluster, traitSet, hints, table);
    }

    @Override public Result implement() {
      LuceneTable t = getTable().unwrap(LuceneTable.class);
      return new Result(t.indexPath, new MatchAllDocsQuery());
    }
  }

  private static class LuceneEnumerableConverter extends ConverterImpl implements EnumerableRel {
    public LuceneEnumerableConverter(RelNode child) {
      super(child.getCluster(),
          ConventionTraitDef.INSTANCE,
          child.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
          child);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new LuceneEnumerableConverter(inputs.get(0));
    }

    @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      try {
        BlockBuilder codeBlock = new BlockBuilder();
        DeclarationStatement fieldStmt =
            Expressions.declare(0, "fields", Expressions.new_(LinkedHashMap.class));
        codeBlock.add(fieldStmt);
        Method putMethod = Map.class.getMethod("put", Object.class, Object.class);
        for (RelDataTypeField f : getRowType().getFieldList()) {
          ConstantExpression fieldName = Expressions.constant(f.getName());
          ConstantExpression fieldType = Expressions.constant(f.getType().getSqlTypeName());
          MethodCallExpression callPut =
              Expressions.call(fieldStmt.parameter, putMethod, fieldName, fieldType);
          codeBlock.add(Expressions.statement(callPut));
        }
        ConstantExpression indexPath =
            Expressions.constant(((LuceneRel) input).implement().indexPath);
        ConstantExpression luceneQuery =
            Expressions.constant(((LuceneRel) input).implement().query.toString());
        NewExpression luceneEnumerable =
            Expressions.new_(LuceneEnumerable.class, indexPath, fieldStmt.parameter, luceneQuery);
        codeBlock.add(Expressions.return_(null, luceneEnumerable));
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
            pref.prefer(JavaRowFormat.ARRAY));
        return implementor.result(physType, codeBlock.toBlock());
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
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
