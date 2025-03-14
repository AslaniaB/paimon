/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.utils.HiveUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.apache.flink.table.hive.LegacyHiveClasses;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** {@link MetaStorePreEventListener} for Hive MetaStore. */
public class PaimonHMSPreEventListener extends MetaStorePreEventListener {

    private static final String INPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonOutputFormat";

    public PaimonHMSPreEventListener(Configuration config) {
        super(config);
    }

    @Override
    public void onEvent(PreEventContext context) {
        if (context.getEventType() == PreEventContext.PreEventType.CREATE_TABLE
                && isPaimonTable(((PreCreateTableEvent) context).getTable())) {
            Table table = ((PreCreateTableEvent) context).getTable();
            try {
                String pathLocation = table.getSd().getLocation();
                Path tablePath = new Path(pathLocation);
                FileIO fileIO = FileIO.get(tablePath, catalogContext(table, pathLocation));
                Optional<TableSchema> tableSchema = new SchemaManager(fileIO, tablePath).latest();
                if (!tableSchema.isPresent()) {
                    throw new IllegalArgumentException(
                            "Schema file not found in location " + pathLocation);
                }
                TableSchema schema = tableSchema.get();
                Map<String, String> tableParameters = table.getParameters();
                List<String> partitionKeys = schema.partitionKeys();
                if (!partitionKeys.isEmpty()) {
                    tableParameters.putIfAbsent(
                            CoreOptions.PARTITION.key(), String.join(",", partitionKeys));
                }
                List<String> bucketKeys = schema.bucketKeys();
                if (!bucketKeys.isEmpty()) {
                    tableParameters.putIfAbsent(
                            CoreOptions.BUCKET_KEY.key(), String.join(",", bucketKeys));
                }
                List<String> primaryKeys = schema.primaryKeys();
                if (!primaryKeys.isEmpty()) {
                    tableParameters.putIfAbsent(
                            CoreOptions.PRIMARY_KEY.key(), String.join(",", primaryKeys));
                }

            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to fill the properties for paimon table - " + table.getTableName(),
                        e);
            }
        }
    }

    public static boolean isPaimonTable(Table table) {
        boolean isPaimonTable =
                INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                        && OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat());
        return isPaimonTable || LegacyHiveClasses.isPaimonTable(table);
    }

    private CatalogContext catalogContext(Table table, String location) {
        Options options = HiveUtils.extractCatalogConfig(getConf());
        options.set(CoreOptions.PATH, location);
        table.getParameters().forEach(options::set);
        return CatalogContext.create(options, getConf());
    }
}
