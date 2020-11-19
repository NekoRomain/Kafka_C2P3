package tp.database;


import java.sql.*;
import java.time.LocalDateTime;

public class Database {

    //private final String url = "jdbc:postgresql://localhost/tpKafka";
    private final String url = "jdbc:postgresql://kandula.db.elephantsql.com:5432/pornlkar";
    //private final String user = "postgres";
    private final String user = "pornlkar";
    private final String password = "mbdpf5meee5IsCGJXyf80q3Gj9-L0lOz";
    private static final String SELECT_ALL_QUERY_GLOBAL = "select * from global";
    private static final String SELECT_ALL_QUERY_COUNTRY = "select * from countries where country_code =?";
    private static final String SELECT_ALL_QUERY_CONFIRMED = "select sum(total_confirmed)/count(*) from countries";
    private static final String SELECT_ALL_QUERY_AVG_DEATHS = "select sum(total_deaths)/count(*) from countries";
    private static final String SELECT_ALL_QUERY_PERCENTS_DEATHS= "select (sum(total_deaths)/sum(total_confirmed))*100 from countries";
    //private final String password = "root";

    //

//    private final String CREATE_TABLE = "DROP TABLE IF EXISTS global; " +
//            "DROP TABLE IF EXISTS countries; " +
//            "CREATE TABLE global(" +
//            "    id INT PRIMARY KEY NOT NULL," +
//            "    new_confirmed bigint," +
//            "    total_confirmed bigint," +
//            "    new_deaths bigint, " +
//            "    total_deaths bigint," +
//            "    new_recovered bigint," +
//            "    total_recovered bigint," +
//            "    date_maj TIMESTAMP" +
//            "); " +
//            "CREATE TABLE countries(" +
//            "    country VARCHAR(200) PRIMARY KEY," +
//            "    country_code VARCHAR(6)," +
//            "    slug VARCHAR(200)," +
//            "    new_confirmed bigint," +
//            "    total_confirmed bigint," +
//            "    new_deaths bigint," +
//            "    total_deaths bigint," +
//            "    new_recovered bigint," +
//            "    total_recovered bigint," +
//            "    date_maj TIMESTAMP" +
//            ");";



    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);

            if (conn != null) {
                System.out.println("Success");
            } else {
                System.out.println("Failed to make connection!");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public String getGlobal() {


        // Step 1: Establishing a Connection
        try (Connection connection = DriverManager.getConnection(url, user, password);
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_GLOBAL);) {
             System.out.println(preparedStatement);
             // Step 3: Execute the query or update query
             ResultSet rs = preparedStatement.executeQuery();

             // Step 4: Process the ResultSet object.
            while(rs.next()) {
            int id = rs.getInt("id");
            int new_confirmed = rs.getInt("new_confirmed");
            int total_confirmed = rs.getInt("total_confirmed");
            int new_deaths = rs.getInt("new_deaths");
            int total_deaths = rs.getInt("total_deaths");
            int new_recovered = rs.getInt("new_recovered");
            int total_recovered = rs.getInt("total_recovered");
            Date date_maj = rs.getDate("date_maj");
            String stringGlobal = ",new_confirmed: " + new_confirmed + ",total_confirmed:" + total_confirmed
                    + ",new_deaths:" + new_deaths + ",total_deaths:" + "," + total_deaths + ",new_recovered:" + new_recovered
                    + ",total_recovered:" + total_recovered + ",date_maj:" + date_maj;
            System.out.println(id + "," + new_confirmed + "," + total_confirmed + "," + new_deaths + ","
                    + total_deaths + "," + new_recovered + "," + total_recovered + "," + date_maj);

            return stringGlobal;
        }
    } catch(
    SQLException e)

    {
        printSQLException(e);
    }
        return "ERRORS:SELECT ERRORS GLOBAL";
}

    public String getCountry(String countryCode) {


        // Step 1: Establishing a Connection
        try (Connection connection = DriverManager.getConnection(url, user, password);
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_COUNTRY);) {
            preparedStatement.setString(1, countryCode);
            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            ResultSet rs = preparedStatement.executeQuery();

            // Step 4: Process the ResultSet object.
            while(rs.next()) {
                String country = rs.getString("country");
                String country_code = rs.getString("country_code");
                String slug = rs.getString("slug");
                int new_confirmed = rs.getInt("new_confirmed");
                int total_confirmed = rs.getInt("total_confirmed");
                int new_deaths = rs.getInt("new_deaths");
                int total_deaths = rs.getInt("total_deaths");
                int new_recovered = rs.getInt("new_recovered");
                int total_recovered = rs.getInt("total_recovered");
                Date date_maj = rs.getDate("date_maj");
                String stringCountry = "country:" + country + ",country_code:" + country_code + ",slug:"
                        + slug + ",new_confirmed: " + new_confirmed + ",total_confirmed:" + total_confirmed
                        + ",new_deaths:" + new_deaths + ",total_deaths:" + "," + total_deaths + ",new_recovered:"
                        + new_recovered + ",total_recovered:" + total_recovered + ",date_maj:" + date_maj;

                System.out.println(country + "," + country_code + "," + slug + "," + new_confirmed + ","
                        + total_confirmed + "," + new_deaths + "," + total_deaths + ","
                        + new_recovered + "," + total_recovered + "," + date_maj);

                return stringCountry;
            }
        } catch(
                SQLException e)

        {
            printSQLException(e);
        }
        return "ERRORS:SELECT ERRORS COUNTRY(V_PAYS)";
    }
    public String getAvgConfirmed() {


        // Step 1: Establishing a Connection
        try (Connection connection = DriverManager.getConnection(url, user, password);
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_CONFIRMED);) {

            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            ResultSet rs = preparedStatement.executeQuery();

            // Step 4: Process the ResultSet object.
            while(rs.next()) {
                double avg = rs.getDouble("?column?");
                String stringAvg = "avg:" + avg ;
                System.out.println(stringAvg);
                return stringAvg;
            }
        } catch(
                SQLException e)

        {
            printSQLException(e);
        }
        return "ERRORS:SELECT ERRORS AVG CONFIRMED";
    }
    public String getAvgDeaths() {

        // Step 1: Establishing a Connection
        try (Connection connection = DriverManager.getConnection(url, user, password);
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_AVG_DEATHS);) {

            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            ResultSet rs = preparedStatement.executeQuery();

            // Step 4: Process the ResultSet object.
            while (rs.next()) {
                double avg = rs.getDouble("?column?");
                String stringAvg = "avg:" + avg;
                System.out.println(stringAvg);
                return stringAvg;
            }
        } catch (
                SQLException e) {
            printSQLException(e);
        }
        return "ERRORS:SELECT ERRORS AVG DEATHS";
    }

    public String getPercentDeaths() {

        // Step 1: Establishing a Connection
        try (Connection connection = DriverManager.getConnection(url, user, password);
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_PERCENTS_DEATHS);) {

            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            ResultSet rs = preparedStatement.executeQuery();

            // Step 4: Process the ResultSet object.
            while (rs.next()) {
                double percent = rs.getDouble("?column?");
                String stringPercent = "Pourcentage de mortalité:" + percent;
                System.out.println(stringPercent);
                return stringPercent;
            }
        } catch (
                SQLException e) {
            printSQLException(e);
        }
        return "ERRORS:SELECT ERRORS PERCENT";
    }
    public static void printSQLException(SQLException ex) {
        for (Throwable e: ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                    t = t.getCause();
                }
            }
        }
    }
}

