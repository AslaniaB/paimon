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

package org.apache.paimon.flink.action.cdc.format.canal.extend;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.canal.CanalRecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The {@code CanalRecordParser} class is responsible for parsing records from the Canal-JSON
 * format. Canal is a database binlog multi-platform consumer, which is used to synchronize data
 * across databases. This parser extracts relevant information from the Canal-JSON format and
 * transforms it into a list of {@link RichCdcMultiplexRecord} objects, which represent the changes
 * captured in the database.
 *
 * <p>The class handles different types of database operations such as INSERT, UPDATE, and DELETE,
 * and generates corresponding {@link RichCdcMultiplexRecord} objects for each operation.
 *
 * <p>Additionally, the parser supports schema extraction, which can be used to understand the
 * structure of the incoming data and its corresponding field types.
 */
public class TimeStampCanalRecordParser extends CanalRecordParser {

    private static final Logger LOG = LoggerFactory.getLogger(TimeStampCanalRecordParser.class);

    private static final String FIELD_ES = "es";
    private static final String FIELD_RECEIVE_TS = "receive_ts";

    private static final String PAIMON_FIELD_ES = "binlog_event_ts";
    private static final String PAIMON_FIELD_RECEIVE_TS = "canal_receive_ts";

    public TimeStampCanalRecordParser(
            TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    protected void processRecord(
            JsonNode jsonNode, RowKind rowKind, List<RichCdcMultiplexRecord> records) {
        String valueEs =
                JsonSerdeUtil.convertValue(root.get(FIELD_ES), new TypeReference<String>() {});
        String valueReceiveEs =
                JsonSerdeUtil.convertValue(
                        root.get(FIELD_RECEIVE_TS), new TypeReference<String>() {});
        RowType.Builder rowTypeBuilder = RowType.builder();
        Map<String, String> rowData = this.extractRowData(jsonNode, rowTypeBuilder);
        rowTypeBuilder.field(PAIMON_FIELD_ES, DataTypes.BIGINT());
        rowData.putIfAbsent(PAIMON_FIELD_ES, valueEs);
        rowTypeBuilder.field(PAIMON_FIELD_RECEIVE_TS, DataTypes.BIGINT());
        rowData.putIfAbsent(PAIMON_FIELD_RECEIVE_TS, valueReceiveEs);
        records.add(createRecord(rowKind, rowData, rowTypeBuilder.build().getFields()));
    }

    @Override
    protected String format() {
        return "ts-canal-json";
    }
}
