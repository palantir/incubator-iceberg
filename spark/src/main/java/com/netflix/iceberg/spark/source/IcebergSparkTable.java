/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.spark.SparkFilters;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.SupportsRead;
import org.apache.spark.sql.sources.v2.SupportsWrite;
import org.apache.spark.sql.sources.v2.TableCapability;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.sources.v2.writer.BatchWrite;
import org.apache.spark.sql.sources.v2.writer.SupportsSaveMode;
import org.apache.spark.sql.sources.v2.writer.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergSparkTable implements SupportsRead, SupportsWrite {

  private final Table table;

  public IcebergSparkTable(Table table) {
    this.table = table;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new IcebergReaderBuilder(table);
  }

  @Override
  public WriteBuilder newWriteBuilder(CaseInsensitiveStringMap options) {
    Optional<String> formatOption = Optional.ofNullable(options.get("iceberg.write.format"));
    if (formatOption.isPresent()) {
      return new IcebergWriterBuilder(table, FileFormat.valueOf(formatOption.get().toUpperCase(Locale.ENGLISH)));
    }
    return new IcebergWriterBuilder(
        table,
        FileFormat.valueOf(table.properties()
            .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
            .toUpperCase(Locale.ENGLISH)));
  }

  @Override
  public String name() {
    return table.location();
  }

  @Override
  public StructType schema() {
    return SparkSchemaUtil.convert(table.schema());
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
  }


  private static class IcebergWriterBuilder implements WriteBuilder,
      SupportsSaveMode {
    private final Table table;
    private final FileFormat fileFormat;

    public IcebergWriterBuilder(Table table, FileFormat fileFormat) {
      this.table = table;
      this.fileFormat = fileFormat;
    }

    @Override
    public BatchWrite buildForBatch() {
      return new Writer(table, fileFormat);
    }

    @Override
    public WriteBuilder mode(SaveMode mode) {
      Preconditions.checkArgument(mode == SaveMode.Append, "Save mode %s is not supported", mode);
      return this;
    }
  }

  private static class IcebergReaderBuilder implements ScanBuilder,
      SupportsPushDownFilters,
      SupportsPushDownRequiredColumns {

    private static final Filter[] NO_FILTERS = new Filter[0];

    private final Table table;
    private Filter[] pushedFilters = NO_FILTERS;
    private List<Expression> filterExpressions = Lists.newArrayList();
    private StructType requestedSchema = null;

    public IcebergReaderBuilder(Table table) {
      this.table = table;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
      List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
      List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

      for (Filter filter : filters) {
        Expression expr = SparkFilters.convert(filter);
        if (expr != null) {
          expressions.add(expr);
          pushed.add(filter);
        }
      }

      this.filterExpressions = expressions;
      this.pushedFilters = pushed.toArray(new Filter[0]);

      // Spark doesn't support residuals per task, so return all filters
      // to get Spark to handle record-level filtering
      return filters;
    }

    @Override
    public Filter[] pushedFilters() {
      return pushedFilters;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
      this.requestedSchema = requiredSchema;
    }

    @Override
    public Scan build() {
      return new Reader(table, filterExpressions, requestedSchema);
    }
  }
}
