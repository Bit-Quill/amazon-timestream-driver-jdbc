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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles table creation and clean up for the integration tests.
 */
class TableManager {
  static String region = "us-east-1";
 static void setRegion(String regionVal) {
    region = regionVal;
  }

  static String getRegion() {
    return region;
  }

  /**
   * Creates a new database if not already existed.
   * Deletes the database if already existed and then creates a new one.
   * @param database Database to be created
   */
  static void createDatabase(String database) {
      final CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
      createDatabaseRequest.setDatabaseName(database);
      try {
          buildWriteClient().createDatabase(createDatabaseRequest);
      } catch (ConflictException e) {
          final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
          deleteDatabaseRequest.setDatabaseName(database);
          try {
              buildWriteClient().deleteDatabase(deleteDatabaseRequest);
          } catch (Exception exception) {
              System.out.println(exception.getMessage());
          }
          buildWriteClient().createDatabase(createDatabaseRequest);
      } catch (Exception e) {
          System.out.println(e.getMessage());
      }

  }

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
          deleteTable(Constants.TABLE_NAME, Constants.DATABASE_NAME);
          buildWriteClient().deleteDatabase(deleteDatabaseRequest);
        }
        buildWriteClient().createDatabase(createDatabaseRequest);
      }
    }
  }

  /**
   * Creates new tables in the database if not already existed.
   * Deletes the table if already existed and then creates a new one.
   * @param tables Tables to be created
   * @param database Database to contain the tables
   */
  static void createTables(String[] tables, String database) {
    for (int i = 1; i < tables.length; i++) {
      createTable(tables[i], database);
    }
  }

  /**
   * Creates new tables in the database if not already existed.
   * Deletes the table if already existed and then creates a new one.
   * @param table Table to be created
   * @param database Database to contain the table
   */
  static void createTable(String table, String database) {
    final CreateTableRequest createTableRequest = new CreateTableRequest();
    createTableRequest.setDatabaseName(database);
    createTableRequest.setTableName(table);
    final RetentionProperties retentionProperties = new RetentionProperties()
            .withMemoryStoreRetentionPeriodInHours(Constants.HT_TTL_HOURS)
            .withMagneticStoreRetentionPeriodInDays(Constants.CT_TTL_DAYS);
    createTableRequest.setRetentionProperties(retentionProperties);
    try {
      buildWriteClient().createTable(createTableRequest);
    } catch (ConflictException e) {
      deleteTable(Constants.TABLE_NAME, Constants.DATABASE_NAME);
      buildWriteClient().createTable(createTableRequest);
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
      deleteTable(Constants.TABLE_NAME, Constants.DATABASE_NAME);
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
   * Deletes the database. Precondition: database is empty
   * @param database Database to be deleted
   */
  static void deleteDatabase(String database) {
    final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest();
    deleteDatabaseRequest.setDatabaseName(database);
    try {
      buildWriteClient().deleteDatabase(deleteDatabaseRequest);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
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
        deleteTable(Constants.TABLE_NAME, Constants.DATABASE_NAME);
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
      deleteDatabase(databases[i]);
    }
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
     * Deletes the tables from database
     *
     * @param tables   Tables to be deleted
     * @param database Database to delete the tables from
     */
    static void deleteTables(String[] tables, String database) {
      for (int i = 1; i < tables.length; i++) {
        deleteTable(tables[i], database);
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
    //return AmazonTimestreamWriteClientBuilder.standard().withRegion("us-east-1").build();
    return AmazonTimestreamWriteClientBuilder.standard().withRegion(getRegion()).build();
  }
}
