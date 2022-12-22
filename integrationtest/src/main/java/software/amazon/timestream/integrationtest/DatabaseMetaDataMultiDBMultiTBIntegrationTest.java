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
class DatabaseMetaDataMultiDBMultiTBIntegrationTest {
  private DatabaseMetaData metaData;
  private Connection connection;

  @BeforeAll
  private static void setUp() {
    TableManager.createDatabases(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES);
    TableManager.createTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[0]);
    TableManager.createTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[1]);
    TableManager.createTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[2]);
  }

  @AfterAll
  private static void cleanUp() {
    TableManager.deleteTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[0]);
    TableManager.deleteTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[1]);
    TableManager.deleteTables(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[2]);
    TableManager.deleteDatabases(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES);
  }

  @BeforeEach
  private void init() throws SQLException {
    final Properties p = new Properties();
    connection = DriverManager.getConnection(Constants.URL, p);
    metaData = connection.getMetaData();
  }

  @AfterEach
  private void terminate() throws SQLException {
    connection.close();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   *
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
   * Test getSchemas returns the databases.
   *
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test retrieving the databases.")
  void testSchemas() throws SQLException {
    final List<String> databasesList = Arrays.asList(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES);
    final List<String> schemasList = new ArrayList<>();
    try (ResultSet schemas = metaData.getSchemas()) {
      while (schemas.next()) {
        schemasList.add(schemas.getString("TABLE_SCHEM"));
      }
    }
    Assertions.assertTrue(schemasList.containsAll(databasesList));
  }

  /**
   * Test getSchemas returns databases when given matching patterns.
   *
   * @param schemaPattern the schema pattern to be tested
   * @param index         index of database name in Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "JD_BC_Int.%, 0",
      "%ion!_T%' escape '!, 0",
      "%DB_001, 0",
      "JDB.C%, 1",
      "%ion-T%, 1",
      "%DB_002, 1",
      "JD-BC%, 2",
      "%ion.T%, 2",
      "%DB!_003' escape '!, 2",
  })
  @DisplayName("Test retrieving database name JD_BC_Int.egration_Test_DB_001, JDB.C_Integration-Test_DB_002, JD-BC_Integration.Test_DB_003 with pattern.")
  void testGetSchemasWithSchemaPattern(String schemaPattern, int index) throws SQLException {
    try (ResultSet schemas = metaData.getSchemas(null, schemaPattern)) {
      Assertions.assertTrue(schemas.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[index], schemas.getString("TABLE_SCHEM"));
    }
  }

  /**
   * Test getTables returns tables from JDBC_Inte.gration_Te.st_DB_01 when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param index         index of table name in Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "%_Table_01_01, %ion!_T%' escape '!, 0",
      "%-gration_Tes1t%, JD_BC_Int.%, 0",
      "_nte-gration_Tes1t_Table_0__01, %DB_001, 0",
      "%_Table_01_02%, %ion!_T%' escape '!, 1",
      "%-gration2_Te-st%, JD_BC_Int.%, 1",
      "_nte-gration2_Te-st_Table_0__02, %DB_001, 1",
      "%_3Ta-ble_01_03%, %ion!_T%' escape '!, 2",
      "%-gration_Test%, JD_BC_Int.%, 2",
      "_nte-gration_Test_3Ta-ble_0__03, %DB_001, 2"
  })
  @DisplayName("Test retrieving Inte-gration_Tes1t_Table_01_01, Inte-gration2_Te-st_Table_01_02, Inte-gration_Test_3Ta-ble_01_03 from JD_BC_Int.egration_Test_DB_001.")
  void testTablesWithPatternFromDB1WithPattern(final String tablePattern, final String schemaPattern, final int index) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, schemaPattern, tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1[index], tableResultSet.getObject("TABLE_NAME"));
    }
  }

  /**
   * Test getTables returns tables from JDBC_Inte.gration_Te.st_DB_01 when given matching patterns.
   *
   * @param tablePattern the table pattern to be tested
   * @param index        index of table name in Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "%_Table_01_01, 0",
      "%-gration_Tes1t%, 0",
      "_nte-gration_Tes1t_Table_0__01, 0",
      "%_Table_01_02%, 1",
      "%-gration2_Te-st%, 1",
      "_nte-gration2_Te-st_Table_0__02, 1",
      "%_3Ta-ble_01_03%, 2",
      "%-gration_Test%, 2",
      "_nte-gration_Test_3Ta-ble_0__03, 2"
  })
  @DisplayName("Test retrieving Inte-gration_Tes1t_Table_01_01, Inte-gration2_Te-st_Table_01_02, Inte-gration_Test_3Ta-ble_01_03 from JD_BC_Int.egration_Test_DB_001.")
  void testTablesWithPatternFromDB1(final String tablePattern, final int index) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[0], tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES1[index], tableResultSet.getObject("TABLE_NAME"));
    }
  }

  /**
   * Test getTables returns tables from Integr.ation_Test_Ta_ble_02 when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param index         index of table name in Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "%-Test%, JDB.C%, 0",
      "%!_02!_01' escape '!, %DB_002,  0",
      "_ntegration_Test_Ta1ble_0__01, %ion-T%, 0",
      "%_Table_02_02, JDB.C%,1",
      "%gration.-Te-st%, %ion-T%, 1",
      "%-Te-st!_%' escape '!, %DB_002, 1"
  })
  @DisplayName("Test retrieving Integration-Test_Ta1ble_02_01, Integration.-Te-st_Table_02_02 from Integr.ation_Test_Ta_ble_02.")
  void testTablesWithPatternFromDB2WithPattern(final String tablePattern, final String schemaPattern, final int index) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, schemaPattern, tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2[index], tableResultSet.getObject("TABLE_NAME"));
    }
  }

  /**
   * Test getTables returns tables from Integr.ation_Test_Ta_ble_02 when given matching patterns.
   *
   * @param tablePattern the table pattern to be tested
   * @param index        index of table name in Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "%-Test%, 0",
      "%!_02!_01' escape '!, 0",
      "_ntegration_Test_Ta1ble_0__01, 0",
      "%_Table_02_02, 1",
      "%gration.-Te-st%, 1",
      "%-Te-st!_%' escape '!, 1"
  })
  @DisplayName("Test retrieving Integration-Test_Ta1ble_02_01, Integration.-Te-st_Table_02_02 from Integr.ation_Test_Ta_ble_02.")
  void testTablesWithPatternFromDB2(final String tablePattern, final int index) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[1], tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2[index], tableResultSet.getObject("TABLE_NAME"));
    }
  }

  /**
   * Test getTables returns tables from JD-BC_Integration.Test_DB_003 when given matching patterns.
   *
   * @param tablePattern  the table pattern to be tested
   * @param schemaPattern the database pattern to be tested
   * @param index         index of table name in Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "%-BC_Integration-Test_Ta1%, JD-BC%, 0",
      "%!_03!_01' escape '!, %ion.T%, 0",
      "JD-BC__ntegration-Test_Ta1ble_0__01, %DB!_003' escape '!, 0",
      "%.-Te-st_T%, %ion.T%, 1",
      "%03_02, JD-BC%, 1",
      "JD-BC!_Integration.-Te-%' escape '!, %DB!_003' escape '!, 1",
      "%--Test%, %ion.T%, 2",
      "%3_03, JD-BC%, 2",
      "%t2!_T%' escape '!, %DB!_003' escape '!, 2",
      "%0-Te-st%, JD-BC%, 3",
      "%_3_04, %ion.T%, 3",
      "%a.ble%' escape '!, %DB!_003' escape '!, 3"
  })
  @DisplayName("Test retrieving JD-BC_Integration-Test_Ta1ble_03_01, JD-BC_Integration.-Te-st_Table_03_02, JD-BC_Integration--Test2_Table_03_03, JD-BC_Integration0-Te-st_Ta.ble_03_04 from JD-BC_Integration.Test_DB_003.")
  void testTablesWithPatternFromDB3WithPattern(final String tablePattern, final String schemaPattern, final int index) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, schemaPattern, tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3[index], tableResultSet.getObject("TABLE_NAME"));
    }
  }

  /**
   * Test getTables returns tables from JD-BC_Integration.Test_DB_003 when given matching patterns.
   *
   * @param tablePattern the table pattern to be tested
   * @param index        index of table name in Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES2
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
      "%-BC_Integration-Test_Ta1%, 0",
      "%!_03!_01' escape '!, 0",
      "JD-BC__ntegration-Test_Ta1ble_0__01, 0",
      "%.-Te-st_T%, 1",
      "%03_02, 1",
      "JD-BC!_Integration.-Te-%' escape '!, 1",
      "%--Test%, 2",
      "%3_03, 2",
      "%t2!_T%' escape '!, 2",
      "%0-Te-st%, 3",
      "%_3_04, 3",
      "%a.ble%' escape '!, 3"
  })
  @DisplayName("Test retrieving JD-BC_Integration-Test_Ta1ble_03_01, JD-BC_Integration.-Te-st_Table_03_02, JD-BC_Integration--Test2_Table_03_03, JD-BC_Integration0-Te-st_Ta.ble_03_04 from JD-BC_Integration.Test_DB_003.")
  void testTablesWithPatternFromDB3(final String tablePattern, final int index) throws SQLException {
    try (ResultSet tableResultSet = metaData.getTables(null, Constants.MULTI_DB_MUTLI_TB_DATABASES_NAMES[2], tablePattern, null)) {
      Assertions.assertTrue(tableResultSet.next());
      Assertions.assertEquals(Constants.MULTI_DB_MUTLI_TB_TABLE_NAMES3[index], tableResultSet.getObject("TABLE_NAME"));
    }
  }
}
