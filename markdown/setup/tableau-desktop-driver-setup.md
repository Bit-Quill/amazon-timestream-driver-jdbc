### Tableau Desktop
[Link to product webpage](https://www.tableau.com/products/desktop).

#### Using Tableau with Generic JDBC driver (without using Connector).
1. [Download](https://github.com/awslabs/amazon-timestream-driver-jdbc/releases/latest) the Timestream JDBC driver fully shaded JAR file (e.g., `amazon-timestream-jdbc-2.0.0-shaded.jar`) and copy it to one of these
   directories according to your operating system:
   - **_Windows_**: `C:\Program Files\Tableau\Drivers`
   ![Example](../images/tableau/tableau-driver-location.png)
    - **_macOS_**: `~/Library/Tableau/Drivers`

2. Open Tableau > Connect > Other Databases (JDBC)

3. Enter **URL**: `jdbc:timestream://PropertyName1=value1;PropertyName2=value2...`. For a list of connection properties (e.g., Access Key, Secret Key, Region, etc.), see [README](../../README.md#optional-connection-properties). Then select **Dialect** as `PostgreSQL` and click **Sign In**.
![Tableau Sign In page](../images/tableau/tableau-sign-in.png)
