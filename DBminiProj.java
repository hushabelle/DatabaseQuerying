import java.io.*;
import java.sql.*;
import java.util.*;

public class DBminiProj {
	public static void main(String[] argv) throws SQLException, IOException {
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter username: ");
		String username = scanner.nextLine();
		System.out.println("Enter password: ");
		String password = scanner.nextLine();

		String database = "teachdb.cs.rhul.ac.uk";
		
		Connection connection = connectToDatabase(username, password, database);
		if (connection != null) {
			System.out.println("SUCCESS: You made it!"
					+ "\n\t You can now take control of your database!\n");
		} else {
			System.out.println("ERROR: \tFailed to make connection!");
			System.exit(1);
		}
		scanner.close();
		
		//dropping and creation of tables
		dropTable(connection, "airport");
		System.out.println("Recreating airport table. . .");
		createTable(connection, "airport (airportCode varChar(3), airportName char(100), City char(30), State char(2), primary key (airportCode))");
		insertIntoTableFromFile(connection, "airport", "src/airport");

		System.out.println("\n");
		dropTable(connection, "delayedFlights");
		System.out.println("Recreating delayedFlights table. . .");
		createTable(connection, "delayedFlights (ID_of_Delayed_Flight int, Month int, DayofMonth int, DayOfWeek int, depTime int, ScheduledDepTime int, ArrTime int, ScheduledArrTime int, UniqueCarrier varChar(2), flightNum int, ActualFlightTime int, scheduledFlightTime int, AirTime int, ArrDelay int, DepDelay int, Orig varChar(3), Dest varChar(3), Distance int, primary key (ID_of_Delayed_Flight))");
		insertIntoTableFromFile(connection, "delayedFlights", "src/delayedFlights");
				
		//QUERIES
		//query1
		String query1 = "SELECT DISTINCT uniqueCarrier, count(*) FROM delayedFlights group by uniqueCarrier order by count desc limit 5";
		System.out.println("\n");
		System.out.println("############ 1st Query ############");
		ResultSet rs = executeQuery(connection, query1);
		while (rs.next()) {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				System.out.print(rs.getString(i) + " ");
				}
			System.out.println();
		}
		
		//query2
		String query2 = "SELECT DISTINCT airport.city, COUNT(Orig) FROM airport, delayedFlights WHERE airport.airportCode = delayedFlights.Orig GROUP BY airport.City ORDER BY count desc limit 5";
		System.out.println("\n");
		System.out.println("############ 2nd Query ############");
		ResultSet rs1 = executeQuery(connection, query2);
		while (rs1.next()) {
			for (int i = 1; i <= rs1.getMetaData().getColumnCount(); i++) {
				System.out.print(rs1.getString(i) + " ");
				}
			System.out.println();
		}
		
		//query3
		String query3 = "SELECT DISTINCT Dest,SUM(ArrDelay) FROM delayedFlights GROUP BY Dest ORDER BY SUM(ArrDelay) DESC LIMIT 5 OFFSET 1";
		System.out.println("\n");
		System.out.println("############ 3rd Query ############");
		ResultSet rs2 = executeQuery(connection, query3);
		while (rs2.next()) {
			for (int i = 1; i <= rs2.getMetaData().getColumnCount(); i++) {
				System.out.print(rs2.getString(i) + " ");
				}
			System.out.println();
		}
		
		//query4
		String query4 = "SELECT state, COUNT(airportCode) FROM airport GROUP BY state HAVING count(airportCode) >= 10  ";
		System.out.println("\n");
		System.out.println("############ 4th Query ############");
		ResultSet rs3 = executeQuery(connection, query4);
		while (rs3.next()) {
			for (int i = 1; i <= rs3.getMetaData().getColumnCount(); i++) {
				System.out.print(rs3.getString(i) + " ");
				}
			System.out.println();
		}
		
		//query5
		String query5 = "SELECT airport.*, COUNT(*) FROM airport INNER JOIN delayedFlights ON airportCode = delayedFlights.Orig GROUP BY airport.airportCode, airport.State ORDER BY COUNT(*) DESC LIMIT 5";
		System.out.println("\n");
		System.out.println("############ 5th Query ############");
		ResultSet rs4 = executeQuery(connection, query5);
		while (rs4.next()) {
			for (int i = 1; i <= rs4.getMetaData().getColumnCount(); i++) {
				System.out.print(rs4.getString(i) + " ");
				}
			System.out.println();
		}
	}
	
	public static ResultSet executeQuery(Connection connection, String query) {
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(query);
			return resultSet;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void createTable(Connection connection, String tableDescription) {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			statement.execute("CREATE TABLE " + tableDescription);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void insertIntoTableFromFile(Connection connection, String table, String filename) {
		String thisLine = "";
	
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			Statement statement = connection.createStatement();	
			while ((thisLine = br.readLine()) != null) {
				String sql = "INSERT INTO " + table + " VALUES (";
				String[] values = thisLine.split(",");
				int i;
				for(i = 0; i < values.length-1; i++) {
					sql += "'" + values[i]+"',";
				}
				sql += "'" + values[i] + "')";
				statement.addBatch(sql);
			}
			statement.executeBatch();
		} catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println("Inserting records into table. . .");
	}
	
	public static void dropTable(Connection connection, String table) {
		//Statement object enables to send and receive data from database
		//will prepare JDBC to expect an SQL query to be executed
		Statement statement = null;
		try {
			statement = connection.createStatement();
			statement.execute("DROP TABLE IF EXISTS " + table);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Dropping " + table + " table. . .");
	}
	
	
	// ADVANCED: This method is for advanced users only. You should not need to change this!
	public static Connection connectToDatabase(String user, String password, String database) {
		System.out.println("------ Testing PostgreSQL JDBC Connection ------");
		Connection connection = null;
		try {
			String protocol = "jdbc:postgresql://";
			String dbName = "/CS2855/";
			String fullURL = protocol + database + dbName + user;
			connection = DriverManager.getConnection(fullURL, user, password);
		} catch (SQLException e) {
			String errorMsg = e.getMessage();
			if (errorMsg.contains("authentication failed")) {
				System.out.println("ERROR: \tDatabase password is incorrect. Have you changed the password string above?");
				System.out.println("\n\tMake sure you are NOT using your university password.\n"
						+ "\tYou need to use the password that was emailed to you!");
			} else {
				System.out.println("Connection failed! Check output console.");
				e.printStackTrace();
			}
		}
		return connection;
	}
}