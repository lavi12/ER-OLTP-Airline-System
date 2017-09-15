import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ConnectToDB {

	public static Statement st = null;
	
    public static Connection ConnectionTest()  {
    	Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.");
            e.printStackTrace();
        }

        // Create property object to hold user name & password
        Properties myProp = new Properties();
        myProp.put("user", "team07");
        myProp.put("password", "june");
        
        try {       	
            conn = DriverManager.getConnection("jdbc:postgresql://129.7.243.243:5432/team07", myProp);
            //System.out.println("connected");
            return conn;
        } catch (SQLException e) {
            System.err.println("Could not connect to database.");
            e.printStackTrace();
        
        }

		return conn;
   }
   public static ResultSet executeQuery(String sqlStatement) {
        try {
            return st.executeQuery(sqlStatement);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
  }
}
