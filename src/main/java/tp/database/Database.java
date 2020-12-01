package tp.database;


import java.sql.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

public class Database {

    private static final Database instance = new Database();

    private static final DecimalFormat df2 = new DecimalFormat(".##");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

    private Connection connection = null;

    private Database() {
        connection = connect();
    }

    /*PARAMETTRE DE CONNEXION A LA BASE DE DONNEES POSTGRE*/
    private final String url = "jdbc:postgresql://127.0.0.1:5432/tpkafka";
    private final String user = "postgres";
    private final String password = "test";

//    private final String url = "jdbc:postgresql://kandula.db.elephantsql.com:5432/pornlkar";
//    private final String user = "pornlkar";
//    private final String password = "mbdpf5meee5IsCGJXyf80q3Gj9-L0lOz";

    private static final String SELECT_ALL_QUERY_GLOBAL = "select * from global";
    private static final String SELECT_ALL_QUERY_COUNTRY = "select * from countries where country_code =?";
    private static final String SELECT_ALL_QUERY_CONFIRMED = "select sum(total_confirmed)/count(*) from countries";
    private static final String SELECT_ALL_QUERY_AVG_DEATHS = "select sum(total_deaths)/count(*) from countries";
    private static final String SELECT_ALL_QUERY_PERCENTS_DEATHS = "select (sum(total_deaths)/sum(total_confirmed))*100 from countries";


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

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_GLOBAL);) {
            System.out.println(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();


            while (rs.next()) {
                int id = rs.getInt("id");
                int new_confirmed = rs.getInt("new_confirmed");
                int total_confirmed = rs.getInt("total_confirmed");
                int new_deaths = rs.getInt("new_deaths");
                int total_deaths = rs.getInt("total_deaths");
                int new_recovered = rs.getInt("new_recovered");
                int total_recovered = rs.getInt("total_recovered");
                Date date_maj = rs.getDate("date_maj");
                String stringGlobal = "New_confirmed: " + new_confirmed + "\nTotal_confirmed: " + total_confirmed
                        + "\nNew_deaths: " + new_deaths + "\nTotal_deaths: " + total_deaths + "\nNew_recovered: "
                        + new_recovered + "\nTotal_recovered: " + total_recovered + "\nDate_maj: " + dateFormat.format(date_maj);
                return stringGlobal;
            }
        } catch (SQLException e) {
            printSQLException(e);
        }
        return "ERRORS:SELECT ERRORS GLOBAL";
    }

    public String getCountry(String countryCode) {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_COUNTRY);) {
            preparedStatement.setString(1, countryCode);
            System.out.println(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
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
                String stringCountry = "Country: " + country + "\nCountry_code: " + country_code + "\nSlug: "
                        + slug + "\nNew_confirmed: " + new_confirmed + "\nTotal_confirmed: " + total_confirmed
                        + "\nNew_deaths: " + new_deaths + "\nTotal_deaths: " + total_deaths + "\nNew_recovered: "
                        + new_recovered + "\nTotal_recovered: " + total_recovered + "\nDate_maj: " + dateFormat.format(date_maj);
                return stringCountry;
            }
        } catch (SQLException e) {
            printSQLException(e);
        }
        return "ERRORS: SELECT ERRORS COUNTRY(V_PAYS)";
    }

    public String getAvgConfirmed() {


        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_CONFIRMED);) {

            System.out.println(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                int avg = rs.getInt("?column?");
                String stringAvg = "Moyenne des cas confirmés: " + avg;
                System.out.println(stringAvg);
                return stringAvg;
            }
        } catch (SQLException e) {
            printSQLException(e);
        }
        return "ERRORS: SELECT ERRORS AVG CONFIRMED";
    }

    public String getAvgDeaths() {


        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_AVG_DEATHS);) {

            System.out.println(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                int avg = rs.getInt("?column?");
                String stringAvg = "Moyenne des morts: " + avg;
                System.out.println(stringAvg);
                return stringAvg;
            }
        } catch (SQLException e) {
            printSQLException(e);
        }
        return "ERRORS: SELECT ERRORS AVG DEATHS";
    }

    public String getPercentDeaths() {


        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL_QUERY_PERCENTS_DEATHS);) {

            System.out.println(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                double percent = rs.getDouble("?column?");
                String stringPercent = "Pourcentage de mortalité: " + df2.format(percent) + " %";
                System.out.println(stringPercent);
                return stringPercent;
            }
        } catch (SQLException e) {
            printSQLException(e);
        }
        return "ERRORS: SELECT ERRORS PERCENT";
    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
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

    public static final Database getInstance() {
        return instance;
    }

    public Connection getConnection() {
        if (connection == null) {
            connection = connect();
        }
        return connection;
    }

    public void closeConnection() throws SQLException {
        if (connection != null)
            connection.close();
    }
}

