/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.timestream.integrationtest;

import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.ConflictException;
import com.amazonaws.services.timestreamwrite.model.CreateDatabaseRequest;
import com.amazonaws.services.timestreamwrite.model.CreateTableRequest;
import com.amazonaws.services.timestreamwrite.model.DeleteDatabaseRequest;
import com.amazonaws.services.timestreamwrite.model.DeleteTableRequest;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.RetentionProperties;
import com.amazonaws.services.timestreamwrite.model.ValidationException;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.google.common.collect.ImmutableList;
import sun.tools.jconsole.JConsole;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles table creation and clean up for the integration tests.
 */
class TableManager {

  /**
   * Creates a new database {@link Constants#DATABASE_NAME} if not
   * already existed. Deletes the database if already existed and then creates a new one.
   */
  static void createDatabases() {
    for (int i = 1; i < Constants.DATABASES_NAMES.length; i++) {
      final CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
      createDatabaseRequest.setDatabaseName(Constants.DATABASES_NAMES[i]);
      try {
        buildWriteClient().createDatabase(createDatabaseRequest);
      } catch (ConflictException e) {
        final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
        deleteDatabaseRequest.setDatabaseName(Constants.DATABASES_NAMES[i]);
        try {
          buildWriteClient().deleteDatabase(deleteDatabaseRequest);
        } catch (ValidationException conflictException) {
          deleteTable();
          buildWriteClient().deleteDatabase(deleteDatabaseRequest);
        }
        buildWriteClient().createDatabase(createDatabaseRequest);
      }
    }
  }

  /**
   * Creates a new table {@link Constants#TABLE_NAME} in the {@link Constants#DATABASE_NAME} if not
   * already existed. Deletes the table if already existed and then creates a new one.
   */
  static void createTable() {
    final CreateTableRequest createTableRequest = new CreateTableRequest();
    createTableRequest.setDatabaseName(Constants.DATABASE_NAME);
    createTableRequest.setTableName(Constants.TABLE_NAME);
    final RetentionProperties retentionProperties = new RetentionProperties()
        .withMemoryStoreRetentionPeriodInHours(Constants.HT_TTL_HOURS)
        .withMagneticStoreRetentionPeriodInDays(Constants.CT_TTL_DAYS);
    createTableRequest.setRetentionProperties(retentionProperties);
    try {
      buildWriteClient().createTable(createTableRequest);
    } catch (ConflictException e) {
      deleteTable();
      buildWriteClient().createTable(createTableRequest);
    }
  }

  /**
   * Populates the table {@link Constants#TABLE_NAME} with records each containing a different
   * Timestream data type.
   */
  static void writeRecords() {
    final long time = System.currentTimeMillis();

    final List<Dimension> dimensions = ImmutableList.of(
        new Dimension().withName("region").withValue("us-east-1"),
        new Dimension().withName("az").withValue("az1"),
        new Dimension().withName("hostname").withValue("host1")
    );

    final List<Record> records = Arrays.stream(MeasureValueType.values())
        .map(val -> new Record()
            .withDimensions(dimensions)
            .withMeasureName(val.toString())
            .withMeasureValueType(val)
            .withMeasureValue(Constants.DATATYPE_VALUE.get(val))
            .withTime(String.valueOf(time)))
        .collect(Collectors.toList());

    final WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
        .withDatabaseName(Constants.DATABASE_NAME)
        .withTableName(Constants.TABLE_NAME)
        .withRecords(records);
    buildWriteClient().writeRecords(writeRecordsRequest);
  }

  /**
   * Deletes the database {@link Constants#DATABASE_NAME}.
   */
  static void deleteDatabases() {
    for (int i = 1; i < Constants.DATABASES_NAMES.length; i++) {
      final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
      deleteDatabaseRequest.setDatabaseName(Constants.DATABASES_NAMES[i]);
      try {
        buildWriteClient().deleteDatabase(deleteDatabaseRequest);
      } catch (ConflictException e) {
        deleteTable();
        buildWriteClient().deleteDatabase(deleteDatabaseRequest);
      }
    }
  }

  /**
   * Deletes the databases in provided array.
   * Precondition: databases are empty
   * @param databases Databases to be deleted
   */
  static void deleteDatabases(String[] databases) {
    for (int i = 1; i < databases.length; i++) {
      final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
      deleteDatabaseRequest.setDatabaseName(databases[i]);
      try {
        buildWriteClient().deleteDatabase(deleteDatabaseRequest);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  /**
   * Deletes the table {@link Constants#TABLE_NAME} from {@link Constants#DATABASE_NAME}.
   */
  static void deleteTable() {
    final DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
    deleteTableRequest.setDatabaseName(Constants.DATABASE_NAME);
    deleteTableRequest.setTableName(Constants.TABLE_NAME);
    buildWriteClient().deleteTable(deleteTableRequest);
  }

  /**
   * Deletes the table from database
   * @param table Table to be deleted
   * @param database Database to delete the table from
   */
  static void deleteTable(String table, String database) {
    final DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
    deleteTableRequest.setDatabaseName(database);
    deleteTableRequest.setTableName(table);
    try {
      buildWriteClient().deleteTable(deleteTableRequest);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * Creates a new Timestream write client.
   * @param region Region
   * @return the {@link AmazonTimestreamWrite}.
   */
  private static AmazonTimestreamWrite buildWriteClient(String region) {
    return AmazonTimestreamWriteClientBuilder.standard().withRegion(region).build();
  }

  /**
   * Creates a new Timestream write client.
   *
   * @return the {@link AmazonTimestreamWrite}.
   */
  private static AmazonTimestreamWrite buildWriteClient() {
    return AmazonTimestreamWriteClientBuilder.standard().withRegion("us-east-1").build();
  }
}
