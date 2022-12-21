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
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//-AL- One database; multiple tables with different naming pattern to test 'LIKE'

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 */
class DatabaseMetaDataOneDBMultiTBIntegrationTest {
  private DatabaseMetaData metaData;
  private Connection connection;

  @BeforeAll
  private static void setUp() {
    TableManager.setRegion(Constants.ONE_DB_MUTLI_TB_REGION);
    TableManager.createDatabase(Constants.ONE_DB_MUTLI_TB_DATABASES_NAME);
    TableManager.createTables(Constants.ONE_DB_MUTLI_TB_TABLE_NAMES, Constants.ONE_DB_MUTLI_TB_DATABASES_NAME);
  }

  @AfterAll
  private static void cleanUp() {
    TableManager.setRegion(Constants.ONE_DB_MUTLI_TB_REGION);
    TableManager.deleteTables(Constants.ONE_DB_MUTLI_TB_TABLE_NAMES, Constants.ONE_DB_MUTLI_TB_DATABASES_NAME);
    TableManager.deleteDatabase(Constants.ONE_DB_MUTLI_TB_DATABASES_NAME);
    //TableManager.deleteDatabase();
    //TableManager.deleteTables();
  }

  @BeforeEach
  private void init() throws SQLException {
    final Properties p = new Properties();
    p.setProperty("Region", Constants.ONE_DB_MUTLI_TB_REGION);
    connection = DriverManager.getConnection(Constants.URL, p);
    metaData = connection.getMetaData();
  }

  @AfterEach
  private void terminate() throws SQLException {
    connection.close();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getCatalogs(). Empty result set should be returned")
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
  @Test
  @DisplayName("Test retrieving the database.")
  void testSchemas() throws SQLException {
    final List<String> databaseList = new ArrayList<>();
    databaseList.add(Constants.ONE_DB_MUTLI_TB_DATABASES_NAME);
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
  @ParameterizedTest
  @ValueSource(strings = {"%_01", "%_Inte.gration%", "%Te/.st_DB"})
  @DisplayName("Test retrieving database name JDBC_Inte.gration_Te.st_DB_01 with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern) throws SQLException {
    try (ResultSet schemas = metaData.getSchemas(null, schemaPattern)) {
      while (schemas.next()) {
        Assertions.assertEquals(Constants.ONE_DB_MUTLI_TB_DATABASES_NAME, schemas.getString("TABLE_SCHEM"));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%tion_Test%", "_ntegrat_%", "%_Test_Table_0_"})
  @DisplayName("Test retrieving Integration_Test_Table_07 from JDBC_Integration07_Test_DB.")
  void testTablesWithPattern(final String pattern) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, null, pattern, null)) {
      while (tableResultSet.next()) {
        Assertions.assertEquals(Constants.TABLE_NAME, tableResultSet.getObject("TABLE_NAME"));
      }
    }
  }
}
