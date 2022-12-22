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

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.timestream.jdbc.TimestreamDatabaseMetaData;

import java.sql.SQLException;

/**
 * Integration tests for supported getters in {@link TimestreamDatabaseMetaData}
 */
class DatabaseMetaDataOneDBOneTBIntegrationTest {

  private DatabaseMetaDataTest dbTest = new DatabaseMetaDataTest(Constants.ONE_DB_ONE_TB_REGION,
                                       Constants.ONE_DB_ONE_TB_DATABASE_NAMES,
                                       Constants.ONE_DB_ONE_TB_TABLE_NAMES);

  @BeforeAll
  private static void setUp() {
    DatabaseMetaDataTest.setUp(
            Constants.ONE_DB_ONE_TB_REGION,
            Constants.ONE_DB_ONE_TB_DATABASE_NAMES,
            Constants.ONE_DB_ONE_TB_TABLE_NAMES);
  }

  @AfterAll
  private static void cleanUp() {
    DatabaseMetaDataTest.cleanUp(
            Constants.ONE_DB_ONE_TB_REGION,
            Constants.ONE_DB_ONE_TB_DATABASE_NAMES,
            Constants.ONE_DB_ONE_TB_TABLE_NAMES);
  }

  @BeforeEach
  private void init() throws SQLException {
    dbTest.init();
  }

  @AfterEach
  private void terminate() throws SQLException {
    dbTest.terminate();
  }

  /**
   * Test getCatalogs returns empty ResultSet.
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test getCatalogs(). Empty result set should be returned")
  void testCatalogs() throws SQLException {
    dbTest.testCatalogs();
  }

  /**
   * Test getSchemas returns the database.
   * @throws SQLException the exception thrown
   */
  @Test
  @DisplayName("Test retrieving the database.")
  void testSchemas() throws SQLException {
    dbTest.testSchemas();
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
    dbTest.testGetSchemasWithSchemaPattern(schemaPattern);
  }

  /**
   * Test getTables returns tables from JDBC_Inte.gration_Te.st_DB_01 when given matching patterns.
   * @param tablePattern the table pattern to be tested
   * @param index index of table name in Constants.ONE_DB_MUTLI_TB_TABLE_NAMES
   * @throws SQLException the exception thrown
   */
  @ParameterizedTest
  @CsvSource(value = {
          "%g/.ration_Test%, 0",
          "_nteg/.rat_%, 0",
          "%_Te_st_T_able_0_, 0",
          "%tion_Test%, 1",
          "_ntegr/.at_%, 1",
          "%_Test_Ta_ble_02, 1",
          "%tion_Tes_t%, 2",
          "_nte/.grat_%, 2",
          "%_Tes_t_Tab_le_, 2"
  })
  @DisplayName("Test retrieving Integ.ration_Te_st_T_able_01, Integr.ation_Test_Ta_ble_02, Inte.gration_Tes_t_Tab_le_03 from JDBC_Inte.gration_Te.st_DB_01.")
  void testTablesWithPattern(final String tablePattern, final int index) throws SQLException {
    dbTest.testTablesWithPattern(tablePattern, index);
  }
}
