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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 */
class DatabaseMetaDataTest {
  private DatabaseMetaData metaData;
  private Connection connection;

  private String region;
  private String[] databases;
  private String[] tables;

  public DatabaseMetaDataTest(String r, String[] ds, String[] ts) {
    region = r;
    databases = ds;
    tables = ts;
  }
  static void setUp(String r, String[] ds, String[] ts) {
    TableManager.setRegion(r);
    TableManager.createDatabases(ds);
    TableManager.createTables(ts, ds);
  }

  static void cleanUp(String r, String[] ds, String[] ts) {
    TableManager.setRegion(r);
    TableManager.deleteTables(ts, ds);
    TableManager.deleteDatabases(ds);
  }
  void init() throws SQLException {
    final Properties p = new Properties();
    p.setProperty("Region", region);
    connection = DriverManager.getConnection(Constants.URL, p);
    metaData = connection.getMetaData();
  }

  void terminate() throws SQLException {
    connection.close();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   * @throws SQLException the exception thrown
   */
  void testCatalogs() throws SQLException {
    final List<String> catalogsList = new ArrayList<>();
    try (ResultSet catalogs = metaData.getCatalogs()) {
      while (catalogs.next()) {
        catalogsList.add(catalogs.getString("TABLE_CAT"));
      }
    }
    Assertions.assertTrue(catalogsList.isEmpty());
  }

  /**
   * Test getSchemas returns the database.
   * @throws SQLException the exception thrown
   */
  public void testSchemas() throws SQLException {
    final List<String> databaseList = Arrays.asList(databases);
    final List<String> schemasList = new ArrayList<>();
    try (ResultSet schemas = metaData.getSchemas()) {
      while (schemas.next()) {
        schemasList.add(schemas.getString("TABLE_SCHEM"));
      }
    }
    Assertions.assertEquals(schemasList, databaseList);
  }

  /**
   * Test getSchemas returns database "JDBC_Inte.gration_Te.st_DB_01" when given matching patterns.
   * @param schemaPattern the schema pattern to be tested
   * @throws SQLException the exception thrown
   */
  public void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    try (ResultSet schemas = metaData.getSchemas(null, schemaPattern)) {
      while (schemas.next()) {
        String schema = schemas.getString("TABLE_SCHEM");
        String match = Arrays
                .stream(databases)
                .filter(x -> x.equals(schema))
                .findFirst()
                .orElse(null);

        Assertions.assertTrue(match != null);
      }
    }
  }

  /**
   * Test getTables returns tables from JDBC_Inte.gration_Te.st_DB_01 when given matching patterns.
   * @param tablePattern the table pattern to be tested
   * @param index index of table name in Constants.ONE_DB_MUTLI_TB_TABLE_NAMES
   * @throws SQLException the exception thrown
   */
  public void testTablesWithPattern(final String tablePattern, final int index) throws SQLException {
    for (String database : databases) {
      try (ResultSet tableResultSet = metaData.getTables(null, database, tablePattern, null)) {
        while (tableResultSet.next()) {
          Assertions.assertEquals(tables[index], tableResultSet.getObject("TABLE_NAME"));
        }
      }
    }
  }
}
