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
import com.netflix.iceberg.Table;
import com.netflix.iceberg.hadoop.HadoopTables;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.TableProvider;
import org.apache.spark.sql.types.StructType;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class IcebergSource implements TableProvider, DataSourceRegister {

  private SparkSession lazySpark = null;
  private Configuration lazyConf = null;

  @Override
  public String shortName() {
    return "iceberg";
  }

  protected Table findTable(CaseInsensitiveStringMap options, Configuration conf) {
    Optional<String> location = Optional.ofNullable(options.get("path"));
    Preconditions.checkArgument(location.isPresent(),
        "Cannot open table without a location: path is not set");

    HadoopTables tables = new HadoopTables(conf);

    return tables.load(location.get());
  }

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  private Configuration lazyBaseConf() {
    if (lazyConf == null) {
      this.lazyConf = lazySparkSession().sparkContext().hadoopConfiguration();
    }
    return lazyConf;
  }

  private Table getTableAndResolveHadoopConfiguration(
      CaseInsensitiveStringMap options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options.asCaseSensitiveMap());
    Table table = findTable(options, conf);
    // Set confs from table properties
    mergeIcebergHadoopConfs(conf, table.properties());
    // Re-overwrite values set in options and table properties but were not in the environment.
    mergeIcebergHadoopConfs(conf, options.asCaseSensitiveMap());
    return table;
  }

  private static void mergeIcebergHadoopConfs(
      Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("iceberg.hadoop"))
        .forEach(key -> baseConf.set(key.replaceFirst("iceberg.hadoop", ""), options.get(key)));
  }

  @Override
  public org.apache.spark.sql.sources.v2.Table getTable(CaseInsensitiveStringMap options) {
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    return new IcebergSparkTable(table);
  }

  @Override
  public org.apache.spark.sql.sources.v2.Table getTable(CaseInsensitiveStringMap options, StructType schema) {
    throw new UnsupportedOperationException("Schema should never be passed into an iceberg table");
  }
}
