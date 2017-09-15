import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public class cleanUpDB {

	public static void cleanTheDB(Connection conn) {
		CallableStatement callsmt = null;
		try {
			callsmt = conn.prepareCall("{ call cleanTheDB() }");
			callsmt.execute();
			callsmt.close();
		} catch (SQLException ex) {
			try {
				callsmt.close();
			} catch (SQLException e1) {
				System.err.println(e1.getMessage());
			}
			System.out.println("Unable to clean up previous tables");
			System.err.println(ex.getMessage());
		} 
	}
}



/*
public static String findFlightID(Connection conn, String org, String dest, Date TripDate) throws SQLException{
//ResultSet rs = CheckConnection.executeQuery(checkNullQuery);
String flightNumber=null;
PreparedStatement pt = null;
ResultSet rs = null;
String queryForFlightID="SELECT A.flightid FROM flight_schedule A, flight B "
		+ "WHERE A.flightid = B.flightid AND A.origin_airportcode='"+org+"' "
		+ "AND A.destination_airportcode='"+dest+"' AND B.flight_date='"+TripDate+"';";
//System.out.println(queryForFlightID);
pt = conn.prepareStatement(queryForFlightID);
rs = pt.executeQuery();
if (!rs.next() ){
	System.out.println("No Flight Found.");
}else{
	do{
		flightNumber=rs.getString(1);
		System.out.println(rs.getString(1));
	}while(rs.next());
}

return flightNumber;
}


 * */

