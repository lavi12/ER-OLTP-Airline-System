import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Random;

public class getRandomData {

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static SecureRandom rnd = new SecureRandom();
	
	//Function to get custID randomly from the DB
	public static String getPassengerID(Connection conn) throws SQLException{
		
		//Get the random customerID by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call getPassengerID( ? ) }");
		st.registerOutParameter(1, Types.VARCHAR);
		st.setInt(2, 100);
		st.execute();
		String  passengerID = st.getString(1);
		st.close();
		return passengerID;
	}
	

	//Function to get getTripDate randomly from the DB
	public static Date getTripDate(Connection conn) throws SQLException{
		
		//Get the random customerID by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call getFlightDate( ? ) }");
		st.registerOutParameter(1, Types.DATE);
		st.setInt(2, 2);
		st.execute();
		Date TripDate = st.getDate(1);
		st.close();
		return TripDate;
	}

	/*//Randomly generate TripType - oneway or roundtrip
	public static String getTripType(){
		String[] ttypes = {"OneWay", "RoundTrip"};
		Random r = new Random();
		String tripType = ttypes[r.nextInt(ttypes.length)];
		return tripType;
	}*/
	
	//Randomly generate Payment Type - Card, Cash, Check
	public static String getPaymentType(){
		String[] ptypes = {"Card", "Cash", "Check"};
		Random r = new Random();
		String paymentType = ptypes[r.nextInt(ptypes.length)];
		return paymentType;
	}
	
	//Get the random reservationid, tripID, paymentID
	public static String randomString( int len ){
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	public static String getSeatType(){
		String[] stypes = {"Economy", "Business"};
		Random r = new Random();
		String seatType = stypes[r.nextInt(stypes.length)];
		return seatType;
	}
	
	public static String[] getOriginAndDestination(Connection conn) throws SQLException{
		String[] airports = new String[4];
		int stops=0;
		//Get the random customerID by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ call getAirportsAndITI( ?, ?, ?,? ) }");
		st.registerOutParameter(1, Types.VARCHAR);
		st.registerOutParameter(2, Types.VARCHAR);
		st.registerOutParameter(3, Types.VARCHAR);
		st.registerOutParameter(4,Types.INTEGER);
		st.execute();
		
		airports[0] = st.getString(1);
		airports[1] = st.getString(2);
		airports[2] = st.getString(3);
		stops=st.getInt(4);
		airports[3]=Integer.toString(stops);
		st.close();
		
		return airports;
	}
	
	//Function to get getTicketType randomly from the DB
	public synchronized static int checkavailability(Connection conn, Date date, String org, String dest, String seatType) throws SQLException{
		//Check availability by calling postgres procedure
		CallableStatement st = conn.prepareCall("{ ? = call checkavailability( ?, ?, ?,? ) }");
		st.registerOutParameter(1, Types.INTEGER);
		st.setDate(2, date);
		st.setString(3, org);
		st.setString(4, dest);
		st.setString(5, seatType);
		st.execute();
		int count = st.getInt(1);
		st.close();
			
		return count;
	}
	
	
	public static String[] getOrgDesFromFLTID(String fltid, Date fltDate,Connection conn){
		String queryGetOrgDes="select origin_airportcode,destination_airportcode from v2 where flight_date='"+fltDate+"' and flightid='"+fltid+"';";
		PreparedStatement prepStmt1 ;
		ResultSet resSet1;
		String places[]=new String[2];
		try {
			prepStmt1=conn.prepareStatement(queryGetOrgDes);
			resSet1 = prepStmt1.executeQuery();
			resSet1.next();
			places[0] = resSet1.getString(1); //airport1
			places[1] = resSet1.getString(2); //airport2
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return places;
	}
	
	public static String[] getOrgDesFromFLTID3(String fltid, Date fltDate,Connection conn){
		String queryGetOrgDes="select origin_airportcode,destination_airportcode from v2 where flight_date='"+fltDate+"' and flightid='"+fltid+"';";
		PreparedStatement prepStmt1 ;
		ResultSet resSet1;
		String places[]=new String[2];
		try {
			prepStmt1=conn.prepareStatement(queryGetOrgDes);
			resSet1 = prepStmt1.executeQuery();
			resSet1.next();
			places[0] = resSet1.getString(1); //
			places[1] = resSet1.getString(2); //
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return places;
	}
	
}
