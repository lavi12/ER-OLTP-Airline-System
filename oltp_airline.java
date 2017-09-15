import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class oltp_airline implements Runnable{

	private Thread t;
	private String threadNumber;

	oltp_airline( String name){
	       threadNumber = name;
	}


	public static void main(String args[]){
		String inp=args[0];
		String temp[]=inp.split("=");
		
		Connection conn =ConnectToDB.ConnectionTest();
		cleanUpDB.cleanTheDB(conn);
			
		ScheduledExecutorService execut = Executors.newSingleThreadScheduledExecutor();
		int nThreadsInput=Integer.parseInt(temp[1]);
	
		Runnable periodicTask = new Runnable() {
		    public void run() {
		        // Invoke method(s) to do the work
		    	for(int i=0; i<nThreadsInput; i++){
		    		oltp_airline T = new oltp_airline("Thread "+i);
				    T.start();
				    
				}
		    }
		};
		
		execut.scheduleAtFixedRate(periodicTask, 0, 10, TimeUnit.SECONDS);
		try {
			conn.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}


	public void run() {
		Connection conn =ConnectToDB.ConnectionTest();
		try {
			String passengerID;
			passengerID=getRandomData.getPassengerID(conn);

			Date TripDate = getRandomData.getTripDate(conn);

			/*String tripType = getRandomData.getTripType();
			System.out.println(tripType);*/

			String paymentType = getRandomData.getPaymentType();

			String reserId = getRandomData.randomString(10);

			String paymentId = getRandomData.randomString(10);

			String tripId = getRandomData.randomString(10);

			String seatType = getRandomData.getSeatType();

			String[] airports = new String[4];
			int stops=0;
			airports = getRandomData.getOriginAndDestination(conn);

			stops=Integer.parseInt(airports[3]);
			//System.out.println("Stops : "+stops);
			if(stops==0){
				noStopTranscation(conn, passengerID, TripDate, reserId, paymentId, tripId, paymentType, airports, seatType);
			}
			if(stops==1){
				oneStopTranscation(conn, passengerID, TripDate, reserId, paymentId, tripId, paymentType, airports, seatType);
			}
			if(stops==2){
				twoStopTranscation(conn, passengerID, TripDate, reserId, paymentId, tripId, paymentType, airports, seatType);
			}

			conn.close();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}finally {
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		
	}
	
	public void start() {
		 if (t == null)
	      {
	         t = new Thread (this, threadNumber);
	         t.start ();
	      }		
	}

	
	public static void noStopTranscation(Connection conn, String passengerID, Date TripDate, String reserId, String paymentId, String tripID, String paymentType, String[] airports, String seatType)
	{

		try{
			PreparedStatement prepStmt = null;
			ResultSet resSet = null;
			PreparedStatement prepStmt1 = null;
			ResultSet resSet1 = null;
			PreparedStatement prepStmt2 = null;
			ResultSet resSet2 = null;
			PreparedStatement prepStmt3 = null;
			ResultSet resSet3 = null;
			PreparedStatement prepStmt4 = null;
			PreparedStatement prepStmt5 = null;
			PreparedStatement prepStmt6 = null;
			PreparedStatement prepStmt7 = null;
			PreparedStatement prepStmt8 = null;
			ResultSet resSet8 = null;
			PreparedStatement prepStmt9 = null;
			PreparedStatement prepStmt10 = null;
			conn.setAutoCommit(false);

			int count = getRandomData.checkavailability(conn, TripDate,airports[0], airports[1],seatType);
			//System.out.println("count "+count);
			if(count==0){
				System.out.println("No Flight on "+TripDate+" from "+airports[0]+" to "+airports[1]+" is available for "+seatType+" class");
			}
			if(count > 0){

				String q1 = "SELECT B.availid FROM tempavail B "+
						"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+seatType+
						"' and B.origin = '"+airports[0]+"' and B.destination = '"+airports[1]+
						"' order by random() LIMIT 1 for Update;";
				//System.out.println(q1);
				prepStmt = conn.prepareStatement(q1);
				resSet = prepStmt.executeQuery();


				if (!resSet.next()) {                           
					System.out.println("There is no seat with seat class "+seatType+" on "+TripDate+" between "+airports[0]+" and "+airports[1]);
				}
				else {
					int avail = resSet.getInt(1);
					//System.out.println("Availability id is : "+avail);
					//Get the flightid
					String queryToChkAvail = "select A.flightid from tempavail A where A.availid = "+avail+";"; 
					prepStmt1 = conn.prepareStatement(queryToChkAvail);
					resSet1 = prepStmt1.executeQuery();
					resSet1.next();
					String flightId = resSet1.getString(1); 
					//System.out.println("Flight id is : "+flightId);
					

					String queryForSeatNum = "select A.seatnum from tempavail A where A.availid = "+avail+";"; 
					//System.out.println(queryForSeatNum);
					prepStmt2 = conn.prepareStatement(queryForSeatNum);
					resSet2 = prepStmt2.executeQuery();
					resSet2.next();
					String flightSeat= resSet2.getString(1); 
					//System.out.println("Flight seat is : "+flightSeat);
					

					String queryForTotalPrice = "select COALESCE(price)+COALESCE(international_tax)+COALESCE(local_tax) from flight_seat_price where flightid='"+flightId+"' and flight_seat_id='"+flightSeat+"' and flight_date='"+TripDate+"';";
					prepStmt3 = conn.prepareStatement(queryForTotalPrice);
					resSet3 = prepStmt3.executeQuery();
					resSet3.next();
					int cst = resSet3.getInt(1);  //Cost of the seat on this flight
					//System.out.println("Flight cost is : "+cst);

					//inserting into payment_info table
					//(payment_id,paymentamount,paymenttype,paymenttimeanddate)
					Date dt = new java.sql.Date(System.currentTimeMillis());
					String queryForPaymentInfo = "INSERT INTO payment_info (payment_id,paymentamount,paymenttype,paymenttimeanddate) values (?, ?, ?, ?);";
					//System.out.println(queryForPaymentInfo);
					prepStmt4 = conn.prepareStatement(queryForPaymentInfo);
					prepStmt4.setString(1, paymentId);
					prepStmt4.setInt(2, cst);
					prepStmt4.setString(3,paymentType);
					prepStmt4.setDate(4, dt);
					prepStmt4.executeUpdate();
					
					
					//inserting into the reservation table
					//(reservation_date, reservation_type_single_round_ , confirmation_number,payment_id,passenger_id)
					String queryForReservation = "INSERT INTO reservation (reservation_date, reservation_type__single_round_ , "
							+ "confirmation_number,payment_id,passenger_id) values (?, ?, ?, ?, ?);";
					//System.out.println(queryForReservation);
					prepStmt5 = conn.prepareStatement(queryForReservation);
					prepStmt5.setDate(1, dt);
					prepStmt5.setString(2, String.valueOf('S'));
					prepStmt5.setString(3, reserId);
					prepStmt5.setString(4, paymentId);
					prepStmt5.setString(5, passengerID);
					prepStmt5.executeUpdate();
					

					//trips
					//(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date)
					String queryForTrips = "INSERT INTO trips(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date) values (?, ?, ?, ?, ?, ?, ?, ?);";
					//System.out.println(queryForTrips);
					prepStmt6 = conn.prepareStatement(queryForTrips);
					prepStmt6.setString(1, reserId);
					prepStmt6.setString(2, airports[2]);
					prepStmt6.setInt(3, 0);
					prepStmt6.setString(4, airports[0]);
					prepStmt6.setString(5,airports[1]);
					prepStmt6.setString(6, "incomplete");
					prepStmt6.setString(7, flightId);
					prepStmt6.setDate(8, TripDate);					
					prepStmt6.executeUpdate();
					
					//insert into the reservation_flight_seat_info table
					//(confirmation_number,itiernary, seat_id_booked, trip_order)
					String queryForResFltInfo = "INSERT INTO reservation_flight_seat_info (confirmation_number,itiernary, seat_id_booked, trip_order) values (?, ?, ?, ?);";
					//System.out.println(queryForResFltInfo );
					prepStmt7 = conn.prepareStatement(queryForResFltInfo );
					prepStmt7.setString(1, reserId);
					prepStmt7.setString(2, airports[2]);
					prepStmt7.setString(3, flightSeat);
					prepStmt7.setInt(4, 0);
					prepStmt7.executeUpdate();
					
					String queryGetAircraftID = "select A.aircraftid from tempavail A where A.availid = "+avail+";"; 
					prepStmt8 = conn.prepareStatement(queryGetAircraftID);
					resSet8 = prepStmt8.executeQuery();
					resSet8.next();
					String planeID = resSet8.getString(1); //Got the aircraft id
					//System.out.println("Aircraft ID  is : "+planeID);
					

					String queryInsertTempBooked = "INSERT INTO tempBookedSeats (availId, passengerID, reservationID, flightDate, flightID, "
							+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
					prepStmt9 = conn.prepareStatement(queryInsertTempBooked);
					prepStmt9.setInt(1, avail);
					prepStmt9.setString(2, passengerID);
					prepStmt9.setString(3, reserId);
					prepStmt9.setDate(4, TripDate);
					prepStmt9.setString(5, flightId);
					prepStmt9.setString(6, flightSeat);
					prepStmt9.setString(7, seatType);
					prepStmt9.setString(8, planeID);
					prepStmt9.setString(9, airports[0]);
					prepStmt9.setString(10, airports[1]);	                
					prepStmt9.executeUpdate();


					String queryDelFromAvail = "DELETE from tempavail A WHERE A.availId = ?";
					prepStmt10 = conn.prepareStatement(queryDelFromAvail);
					prepStmt10.setInt(1, avail);
					prepStmt10.executeUpdate();

					conn.commit();
					
					System.out.println("Booked seat number "+flightSeat+" with reservation ID "+reserId+" on flight "+airports[0]+" to "+airports[1]);
				}
			}
		}catch(Exception ex){
			try {
				System.err.println(ex.getMessage());
				conn.rollback();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	
	public static void oneStopTranscation(Connection conn, String passengerID, Date TripDate, String reserId, 
			String paymentId, String tripID, String paymentType, String[] airports, String seatType)
	{

		try{
			//getting the connecting flights
			String flightsIDs[] =new String[2];
			String connection1[]=new String[2];
			String connection2[]=new String[2];
			CallableStatement st = conn.prepareCall("{ call get2connectingFlights( ?, ? ,?, ?) }");
			st.registerOutParameter(3, Types.VARCHAR);
			st.registerOutParameter(4,Types.VARCHAR);
			st.setString(1, airports[2]);
			st.setDate(2,TripDate);
			st.execute();

			flightsIDs[0]=st.getString(3);
			flightsIDs[1]=st.getString(4);
			st.close();
			
			//System.out.println(flightsIDs[0]+"------"+flightsIDs[1]);
			connection1=getRandomData.getOrgDesFromFLTID(flightsIDs[0], TripDate, conn);
			connection2=getRandomData.getOrgDesFromFLTID(flightsIDs[1], TripDate, conn);	
			//System.out.println(connection1[0]+"-----"+connection1[1]);
			//System.out.println(connection2[0]+"-----"+connection2[1]);
			
			PreparedStatement prepStmt = null;
			ResultSet resSet = null;
			PreparedStatement prepStmt1 = null;
			ResultSet resSet1 = null;
			PreparedStatement prepStmt2 = null;
			ResultSet resSet2 = null;
			PreparedStatement prepStmt3 = null;
			ResultSet resSet3 = null;
			PreparedStatement prepStmt4 = null;
			ResultSet resSet4 = null;
			PreparedStatement prepStmt5 = null;
			ResultSet resSet5 = null;
			PreparedStatement prepStmt6 = null;
			ResultSet resSet6 = null;
			PreparedStatement prepStmt7 = null;
			ResultSet resSet7 = null;
			PreparedStatement prepStmt8 = null;
			ResultSet resSet8 = null;
			PreparedStatement prepStmt9 = null;
			PreparedStatement prepStmt10 = null;
			PreparedStatement prepStmt11 = null;
			ResultSet resSet11 = null;
			PreparedStatement prepStmt12 = null;
			ResultSet resSet12 = null;
			PreparedStatement prepStmt13 = null;
			ResultSet resSet13=null;
			PreparedStatement prepStmt14 = null;
			PreparedStatement prepStmt15 = null;
			PreparedStatement prepStmt16 = null;
			PreparedStatement prepStmt17 = null;
			
			conn.setAutoCommit(false);
			
			int countSeatForConnection1 = getRandomData.checkavailability(conn, TripDate, connection1[0], connection1[1], seatType);
			int countSeatForConnection2 = getRandomData.checkavailability(conn, TripDate, connection2[0], connection2[1], seatType);
						
			if (countSeatForConnection1>0 && countSeatForConnection2>0){
				
				String queryToChkAvailConnection1 = "SELECT B.availid FROM tempavail B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+seatType+
							"' and B.origin = '"+connection1[0]+"' and B.destination = '"+connection1[1]+
							"' order by random() LIMIT 1 for Update;";
				prepStmt= conn.prepareStatement(queryToChkAvailConnection1);
				resSet = prepStmt.executeQuery();

				String queryToChkAvailConnection2 = "SELECT B.availid FROM tempavail B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+seatType+
							"' and B.origin = '"+connection2[0]+"' and B.destination = '"+connection2[1]+
							"' order by random() LIMIT 1 for Update;";
				prepStmt1 = conn.prepareStatement(queryToChkAvailConnection2);
	            resSet1 = prepStmt1.executeQuery();
	            
	            //System.out.println(queryToChkAvailConnection1);
	            //System.out.println(queryToChkAvailConnection2);	            	
       
	            if (!resSet.next() || !resSet1.next()) {                            //if rs.next() returns false then there are no rows.
	            	System.out.println("There is no seat with "+seatType +" from "+connection1[0]+" -> "+connection1[1]+" and "+connection2[0]+" -> "+connection2[1]);
	            }else { 

	            	int avail1 = resSet.getInt(1);
	            	//System.out.println("Availability id is Flight 1: "+avail1);

	            	int avail2 = resSet1.getInt(1);
	            	//System.out.println("Availability id is Flight 2: "+avail2);               
	                

					String queryForSeatNum1 = "select A.seatnum from tempavail A where A.availid = "+avail1+";"; 
					//System.out.println(queryForSeatNum);
					prepStmt2 = conn.prepareStatement(queryForSeatNum1);
					resSet2 = prepStmt2.executeQuery();
					resSet2.next();
					String flightSeat1= resSet2.getString(1); 
					//System.out.println("Flight seat is : "+flightSeat1);
					

					String queryForSeatNum2 = "select A.seatnum from tempavail A where A.availid = "+avail2+";"; 
					//System.out.println(queryForSeatNum);
					prepStmt3 = conn.prepareStatement(queryForSeatNum2);
					resSet3 = prepStmt3.executeQuery();
					resSet3.next();
					String flightSeat2= resSet3.getString(1); 
					//System.out.println("Flight seat is : "+flightSeat2);
					

					String queryForTotalPrice1 = "select COALESCE(price)+COALESCE(international_tax)+COALESCE(local_tax) from flight_seat_price where flightid='"+flightsIDs[0]+"' and flight_seat_id='"+flightSeat1+"' and flight_date='"+TripDate+"';";
					prepStmt4 = conn.prepareStatement(queryForTotalPrice1);
					resSet4 = prepStmt4.executeQuery();
					resSet4.next();
					float cst1 = resSet4.getFloat(1);
					//System.out.println("Flight cost is : "+cst1);
					

					String queryForTotalPrice2 = "select COALESCE(price)+COALESCE(international_tax)+COALESCE(local_tax) from flight_seat_price where flightid='"+flightsIDs[1]+"' and flight_seat_id='"+flightSeat2+"' and flight_date='"+TripDate+"';";
					prepStmt5 = conn.prepareStatement(queryForTotalPrice2);
					resSet5 = prepStmt5.executeQuery();
					resSet5.next();
					float cst2 = resSet5.getFloat(1); 
					//System.out.println("Flight cost is : "+cst2);
					float total_cost=cst1+cst2;
					
					//inserting into payment_info table
					//(payment_id,paymentamount,paymenttype,paymenttimeanddate)
					Date dt = new java.sql.Date(System.currentTimeMillis());
					String queryForPaymentInfo = "INSERT INTO payment_info (payment_id,paymentamount,paymenttype,paymenttimeanddate) values (?, ?, ?, ?);";
					//System.out.println(queryForPaymentInfo);
					prepStmt6 = conn.prepareStatement(queryForPaymentInfo);
					prepStmt6.setString(1, paymentId);
					prepStmt6.setFloat(2, total_cost);
					prepStmt6.setString(3,paymentType);
					prepStmt6.setDate(4, dt);
					prepStmt6.executeUpdate();
					
					
					//inserting into the reservation table
					//(reservation_date, reservation_type_single_round_ , confirmation_number,payment_id,passenger_id)
					String queryForReservation = "INSERT INTO reservation (reservation_date, reservation_type__single_round_ , "
							+ "confirmation_number,payment_id,passenger_id) values (?, ?, ?, ?, ?);";
					//System.out.println(queryForReservation);
					prepStmt7 = conn.prepareStatement(queryForReservation);
					prepStmt7.setDate(1, dt);
					prepStmt7.setString(2, String.valueOf('S'));
					prepStmt7.setString(3, reserId);
					prepStmt7.setString(4, paymentId);
					prepStmt7.setString(5, passengerID);
					prepStmt7.executeUpdate();
					
					
					//trips
					//(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date)
					
					String queryForTrips1 = "INSERT INTO trips(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date) values (?, ?, ?, ?, ?, ?, ?, ?);";
					//System.out.println(queryForTrips);
					prepStmt8 = conn.prepareStatement(queryForTrips1);
					prepStmt8.setString(1, reserId);
					prepStmt8.setString(2, airports[2]);
					prepStmt8.setInt(3, 1);
					prepStmt8.setString(4, connection1[0]);
					prepStmt8.setString(5,connection1[1]);
					prepStmt8.setString(6, "incomplete");
					prepStmt8.setString(7, flightsIDs[0]);
					prepStmt8.setDate(8, TripDate);					
					prepStmt8.executeUpdate();
					
					//trips
					//(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date)
					
					String queryForTrips2 = "INSERT INTO trips(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date) values (?, ?, ?, ?, ?, ?, ?, ?);";
					//System.out.println(queryForTrips);
					prepStmt9 = conn.prepareStatement(queryForTrips2);
					prepStmt9.setString(1, reserId);
					prepStmt9.setString(2, airports[2]);
					prepStmt9.setInt(3, 2);
					prepStmt9.setString(4, connection2[0]);
					prepStmt9.setString(5,connection2[1]);
					prepStmt9.setString(6, "incomplete");
					prepStmt9.setString(7, flightsIDs[1]);
					prepStmt9.setDate(8, TripDate);					
					prepStmt9.executeUpdate();
					
					//insert into the reservation_flight_seat_info table
					//(confirmation_number,itiernary, seat_id_booked, trip_order)
					String queryForResFltInfo1 = "INSERT INTO reservation_flight_seat_info (confirmation_number,itiernary, seat_id_booked, trip_order) values (?, ?, ?, ?);";
					//System.out.println(queryForResFltInfo );
					prepStmt10 = conn.prepareStatement(queryForResFltInfo1);
					prepStmt10.setString(1, reserId);
					prepStmt10.setString(2, airports[2]);
					prepStmt10.setString(3, flightSeat1);
					prepStmt10.setInt(4, 1);
					prepStmt10.executeUpdate();
					
					//insert into the reservation_flight_seat_info table
					//(confirmation_number,itiernary, seat_id_booked, trip_order)
					String queryForResFltInfo2 = "INSERT INTO reservation_flight_seat_info (confirmation_number,itiernary, seat_id_booked, trip_order) values (?, ?, ?, ?);";
					//System.out.println(queryForResFltInfo );
					prepStmt11 = conn.prepareStatement(queryForResFltInfo2);
					prepStmt11.setString(1, reserId);
					prepStmt11.setString(2, airports[2]);
					prepStmt11.setString(3, flightSeat2);
					prepStmt11.setInt(4, 2);
					prepStmt11.executeUpdate();
					
					String queryGetAircraftID1 = "select A.aircraftid from tempavail A where A.availid = "+avail1+";"; 
					prepStmt12 = conn.prepareStatement(queryGetAircraftID1);
					resSet12 = prepStmt12.executeQuery();
					resSet12.next();
					String planeID1 = resSet12.getString(1); //Got the aircraft id
					//System.out.println("Aircraft ID  is : "+planeID1);
					
					String queryGetAircraftID2 = "select A.aircraftid from tempavail A where A.availid = "+avail2+";"; 
					prepStmt13 = conn.prepareStatement(queryGetAircraftID2);
					resSet13 = prepStmt13.executeQuery();
					resSet13.next();
					String planeID2 = resSet13.getString(1); //Got the aircraft id
					//System.out.println("Aircraft ID  is : "+planeID2);
					
					String queryInsertTempBooked1 = "INSERT INTO tempBookedSeats (availId, passengerID, reservationID, flightDate, flightID, "
							+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
					prepStmt14 = conn.prepareStatement(queryInsertTempBooked1);
					prepStmt14.setInt(1, avail1);
					prepStmt14.setString(2, passengerID);
					prepStmt14.setString(3, reserId);
					prepStmt14.setDate(4, TripDate);
					prepStmt14.setString(5, flightsIDs[0]);
					prepStmt14.setString(6, flightSeat1);
					prepStmt14.setString(7, seatType);
					prepStmt14.setString(8, planeID1);
					prepStmt14.setString(9, connection1[0]);
					prepStmt14.setString(10, connection1[1]);	                
					prepStmt14.executeUpdate();
					
					String queryInsertTempBooked2 = "INSERT INTO tempBookedSeats (availId, passengerID, reservationID, flightDate, flightID, "
							+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
					prepStmt15 = conn.prepareStatement(queryInsertTempBooked2);
					prepStmt15.setInt(1, avail2);
					prepStmt15.setString(2, passengerID);
					prepStmt15.setString(3, reserId);
					prepStmt15.setDate(4, TripDate);
					prepStmt15.setString(5, flightsIDs[1]);
					prepStmt15.setString(6, flightSeat2);
					prepStmt15.setString(7, seatType);
					prepStmt15.setString(8, planeID2);
					prepStmt15.setString(9, connection2[0]);
					prepStmt15.setString(10, connection2[1]);	                
					prepStmt15.executeUpdate();
					
					String queryDelFromAvail1 = "DELETE from tempavail A WHERE A.availId = ?";
					prepStmt16 = conn.prepareStatement(queryDelFromAvail1);
					prepStmt16.setInt(1, avail1);
					prepStmt16.executeUpdate();
				
					String queryDelFromAvail2 = "DELETE from tempavail A WHERE A.availId = ?";
					prepStmt17 = conn.prepareStatement(queryDelFromAvail2);
					prepStmt17.setInt(1, avail2);
					prepStmt17.executeUpdate();

					conn.commit();
					System.out.println("Booked seat number "+flightSeat1+" with reservation ID "+reserId+" on flight "+connection1[0]+" to "+connection1[1]+",\n" +"and booked seat number "+flightSeat2+" on flight "+connection2[0]+" to "+connection2[1]+" for a one stop flight between "+connection1[0]+" and "+connection2[1]);
					System.out.println();
	            }
			}
		}catch(Exception ex){
			try {
				System.err.println(ex.getMessage());
				conn.rollback();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static void twoStopTranscation(Connection conn, String passengerID, Date TripDate, String reserId, 
			String paymentId, String tripID, String paymentType, String[] airports, String seatType)
	{
		try{
						
			PreparedStatement prepStmt = null;
			ResultSet resSet = null;
			PreparedStatement prepStmt1 = null;
			ResultSet resSet1 = null;
			PreparedStatement prepStmt2 = null;
			ResultSet resSet2 = null;
			//PreparedStatement prepStmt3 = null;
			//ResultSet resSet3 = null;
			PreparedStatement prepStmt4 = null;
			ResultSet resSet4 = null;
			PreparedStatement prepStmt5 = null;
			ResultSet resSet5 = null;
			PreparedStatement prepStmt6 = null;
			ResultSet resSet6 = null;
			PreparedStatement prepStmt7 = null;
			ResultSet resSet7 = null;
			PreparedStatement prepStmt8 = null;
			ResultSet resSet8 = null;
			PreparedStatement prepStmt9 = null;
			ResultSet resSet9 = null;
			PreparedStatement prepStmt10 = null;
			ResultSet resSet10 = null;
			PreparedStatement prepStmt11 = null;
			ResultSet resSet11 = null;
			PreparedStatement prepStmt12 = null;
			ResultSet resSet12 = null;
			PreparedStatement prepStmt13 = null;
			ResultSet resSet13=null;
			PreparedStatement prepStmt14 = null;
			ResultSet resSet14 = null;
			PreparedStatement prepStmt15 = null;
			ResultSet resSet15 = null;
			PreparedStatement prepStmt16 = null;
			ResultSet resSet16 = null;
			PreparedStatement prepStmt17 = null;
			ResultSet resSet17 = null;
			PreparedStatement prepStmt18 = null;
			ResultSet resSet18 = null;
			PreparedStatement prepStmt19 = null;
			ResultSet resSet19 = null;
			PreparedStatement prepStmt20 = null;
			ResultSet resSet20 = null;
			PreparedStatement prepStmt21 = null;
			PreparedStatement prepStmt22 = null;
			PreparedStatement prepStmt23 = null;
			PreparedStatement prepStmt24 = null;
			PreparedStatement prepStmt25 = null;
			PreparedStatement prepStmt26 = null;
			
			//getting the connecting flights
			String flightsIDs[] =new String[3];
			String connection1[]=new String[2];
			String connection2[]=new String[2];
			String connection3[]=new String[2];
			CallableStatement st = conn.prepareCall("{ call get3connectingFlights( ?, ? ,?, ?, ?) }");
			st.registerOutParameter(3, Types.VARCHAR);
			st.registerOutParameter(4,Types.VARCHAR);
			st.registerOutParameter(5,Types.VARCHAR);
			st.setString(1, airports[2]);
			st.setDate(2,TripDate);
			st.execute();

			flightsIDs[0]=st.getString(3);
			flightsIDs[1]=st.getString(4);
			flightsIDs[2]=st.getString(5);
			st.close();
			
			//System.out.println(flightsIDs[0]+"------"+flightsIDs[1]+"---------------"+flightsIDs[2]);
			connection1=getRandomData.getOrgDesFromFLTID(flightsIDs[0], TripDate, conn);
			connection2=getRandomData.getOrgDesFromFLTID(flightsIDs[1], TripDate, conn);
			connection3=getRandomData.getOrgDesFromFLTID(flightsIDs[2], TripDate, conn);
			//System.out.println(connection1[0]+"-----"+connection1[1]);
			//System.out.println(connection2[0]+"-----"+connection2[1]);
			//System.out.println(connection3[0]+"-----"+connection3[1]);
			
			conn.setAutoCommit(false);
			
			int countSeatForConnection1 = getRandomData.checkavailability(conn, TripDate, connection1[0], connection1[1], seatType);
			int countSeatForConnection2 = getRandomData.checkavailability(conn, TripDate, connection2[0], connection2[1], seatType);
			int countSeatForConnection3 = getRandomData.checkavailability(conn, TripDate, connection3[0], connection3[1], seatType);			
			if (countSeatForConnection1>0 && countSeatForConnection2>0 && countSeatForConnection3>0){

				String queryToChkAvailConnection1 = "SELECT B.availid FROM tempavail B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+seatType+
							"' and B.origin = '"+connection1[0]+"' and B.destination = '"+connection1[1]+
							"' order by random() LIMIT 1 for Update;";
				prepStmt= conn.prepareStatement(queryToChkAvailConnection1);
				resSet = prepStmt.executeQuery();

				String queryToChkAvailConnection2 = "SELECT B.availid FROM tempavail B "+
							"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+seatType+
							"' and B.origin = '"+connection2[0]+"' and B.destination = '"+connection2[1]+
							"' order by random() LIMIT 1 for Update;";
				prepStmt1 = conn.prepareStatement(queryToChkAvailConnection2);
	            resSet1 = prepStmt1.executeQuery();
	            
	            String queryToChkAvailConnection3 = "SELECT B.availid FROM tempavail B "+
						"where B.flightdate = '"+TripDate+"' and B.seatclass = '"+seatType+
						"' and B.origin = '"+connection3[0]+"' and B.destination = '"+connection3[1]+
						"' order by random() LIMIT 1 for Update;";
	            prepStmt2 = conn.prepareStatement(queryToChkAvailConnection3);
	            resSet2 = prepStmt2.executeQuery();
            	
        
	            if (!resSet.next() || !resSet1.next() || !resSet2.next()) {                            //if rs.next() returns false then there are no rows.
	            	System.out.println("There is no "+seatType +" class seat from "+connection1[0]+" -> "+connection1[1]+" and "+connection2[0]+" -> "+connection2[1]+" and "+connection3[0]+"----->"+connection3[1]);
	            }else { 

	            	int avail1 = resSet.getInt(1);
	            	//System.out.println("Availability id is Flight 1: "+avail1);

	            	int avail2 = resSet1.getInt(1);
	            	//System.out.println("Availability id is Flight 2: "+avail2);  
	            	
	            	int avail3 = resSet2.getInt(1);
	            	//System.out.println("Availability id is Flight 3: "+avail3); 
	            	
					String queryForSeatNum1 = "select A.seatnum from tempavail A where A.availid = "+avail1+";"; 
					//System.out.println(queryForSeatNum);
					prepStmt4 = conn.prepareStatement(queryForSeatNum1);
					resSet4 = prepStmt4.executeQuery();
					resSet4.next();
					String flightSeat1= resSet4.getString(1);
					//System.out.println("Flight seat is : "+flightSeat1);
					
					String queryForSeatNum2 = "select A.seatnum from tempavail A where A.availid = "+avail2+";"; 
					prepStmt5 = conn.prepareStatement(queryForSeatNum2);
					resSet5 = prepStmt5.executeQuery();
					resSet5.next();
					String flightSeat2= resSet5.getString(1);
					//System.out.println("Flight seat is : "+flightSeat2);
					
					//Get the flight3 seat number
					String queryForSeatNum3 = "select A.seatnum from tempavail A where A.availid = "+avail3+";"; 
					prepStmt6 = conn.prepareStatement(queryForSeatNum3);
					resSet6 = prepStmt6.executeQuery();
					resSet6.next();
					String flightSeat3= resSet6.getString(1);
					//System.out.println("Flight seat is : "+flightSeat3);
					
					String queryForTotalPrice1 = "select COALESCE(price)+COALESCE(international_tax)+COALESCE(local_tax) from flight_seat_price where flightid='"+flightsIDs[0]+"' and flight_seat_id='"+flightSeat1+"' and flight_date='"+TripDate+"';";
					prepStmt7 = conn.prepareStatement(queryForTotalPrice1);
					resSet7 = prepStmt7.executeQuery();
					resSet7.next();
					float cst1 = resSet7.getFloat(1);
					//System.out.println("Flight cost is : "+cst1);
					
					String queryForTotalPrice2 = "select COALESCE(price)+COALESCE(international_tax)+COALESCE(local_tax) from flight_seat_price where flightid='"+flightsIDs[1]+"' and flight_seat_id='"+flightSeat2+"' and flight_date='"+TripDate+"';";
					prepStmt8 = conn.prepareStatement(queryForTotalPrice2);
					resSet8 = prepStmt8.executeQuery();
					resSet8.next();
					float cst2 = resSet8.getFloat(1); 
					//System.out.println("Flight cost is : "+cst2);
					
					String queryForTotalPrice3 = "select COALESCE(price)+COALESCE(international_tax)+COALESCE(local_tax) from flight_seat_price where flightid='"+flightsIDs[1]+"' and flight_seat_id='"+flightSeat2+"' and flight_date='"+TripDate+"';";
					prepStmt9 = conn.prepareStatement(queryForTotalPrice3);
					resSet9 = prepStmt9.executeQuery();
					resSet9.next();
					float cst3 = resSet9.getFloat(1);
					
					float total_cost=cst1+cst2 +cst3;
					
					//inserting into payment_info table
					//(payment_id,paymentamount,paymenttype,paymenttimeanddate)
					Date dt = new java.sql.Date(System.currentTimeMillis());
					String queryForPaymentInfo = "INSERT INTO payment_info (payment_id,paymentamount,paymenttype,paymenttimeanddate) values (?, ?, ?, ?);";
					//System.out.println(queryForPaymentInfo);
					prepStmt10 = conn.prepareStatement(queryForPaymentInfo);
					prepStmt10.setString(1, paymentId);
					prepStmt10.setFloat(2, total_cost);
					prepStmt10.setString(3,paymentType);
					prepStmt10.setDate(4, dt);
					prepStmt10.executeUpdate();
					
					
					//inserting into the reservation table
					//(reservation_date, reservation_type_single_round_ , confirmation_number,payment_id,passenger_id)
					String queryForReservation = "INSERT INTO reservation (reservation_date, reservation_type__single_round_ , "
							+ "confirmation_number,payment_id,passenger_id) values (?, ?, ?, ?, ?);";
					//System.out.println(queryForReservation);
					prepStmt11 = conn.prepareStatement(queryForReservation);
					prepStmt11.setDate(1, dt);
					prepStmt11.setString(2, String.valueOf('S'));
					prepStmt11.setString(3, reserId);
					prepStmt11.setString(4, paymentId);
					prepStmt11.setString(5, passengerID);
					prepStmt11.executeUpdate();
					
					
					//trips
					//(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date)
					//For connection1
					
					String queryForTrips1 = "INSERT INTO trips(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date) values (?, ?, ?, ?, ?, ?, ?, ?);";
					//System.out.println(queryForTrips);
					prepStmt12 = conn.prepareStatement(queryForTrips1);
					prepStmt12.setString(1, reserId);
					prepStmt12.setString(2, airports[2]);
					prepStmt12.setInt(3, 1);
					prepStmt12.setString(4, connection1[0]);
					prepStmt12.setString(5,connection1[1]);
					prepStmt12.setString(6, "incomplete");
					prepStmt12.setString(7, flightsIDs[0]);
					prepStmt12.setDate(8, TripDate);					
					prepStmt12.executeUpdate();
					
					//trips
					//(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date)
					//For connection2
					
					String queryForTrips2 = "INSERT INTO trips(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date) values (?, ?, ?, ?, ?, ?, ?, ?);";
					//System.out.println(queryForTrips);
					prepStmt13 = conn.prepareStatement(queryForTrips2);
					prepStmt13.setString(1, reserId);
					prepStmt13.setString(2, airports[2]);
					prepStmt13.setInt(3, 2);
					prepStmt13.setString(4, connection2[0]);
					prepStmt13.setString(5,connection2[1]);
					prepStmt13.setString(6, "incomplete");
					prepStmt13.setString(7, flightsIDs[1]);
					prepStmt13.setDate(8, TripDate);					
					prepStmt13.executeUpdate();
					
					String queryForTrips3 = "INSERT INTO trips(confirmation_number,itiernary, trip_order,origin_citycode,destination_citycode,trip_status,flightid,flight_date) values (?, ?, ?, ?, ?, ?, ?, ?);";
					//System.out.println(queryForTrips);
					prepStmt14 = conn.prepareStatement(queryForTrips3);
					prepStmt14.setString(1, reserId);
					prepStmt14.setString(2, airports[2]);
					prepStmt14.setInt(3, 3);
					prepStmt14.setString(4, connection3[0]);
					prepStmt14.setString(5,connection3[1]);
					prepStmt14.setString(6, "incomplete");
					prepStmt14.setString(7, flightsIDs[2]);
					prepStmt14.setDate(8, TripDate);					
					prepStmt14.executeUpdate();
					
					//insert into the reservation_flight_seat_info table
					//(confirmation_number,itiernary, seat_id_booked, trip_order)
					String queryForResFltInfo1 = "INSERT INTO reservation_flight_seat_info (confirmation_number,itiernary, seat_id_booked, trip_order) values (?, ?, ?, ?);";
					//System.out.println(queryForResFltInfo );
					prepStmt15 = conn.prepareStatement(queryForResFltInfo1);
					prepStmt15.setString(1, reserId);
					prepStmt15.setString(2, airports[2]);
					prepStmt15.setString(3, flightSeat1);
					prepStmt15.setInt(4, 1);
					prepStmt15.executeUpdate();
					
					//insert into the reservation_flight_seat_info table
					//(confirmation_number,itiernary, seat_id_booked, trip_order)
					String queryForResFltInfo2 = "INSERT INTO reservation_flight_seat_info (confirmation_number,itiernary, seat_id_booked, trip_order) values (?, ?, ?, ?);";
					//System.out.println(queryForResFltInfo );
					prepStmt16 = conn.prepareStatement(queryForResFltInfo2);
					prepStmt16.setString(1, reserId);
					prepStmt16.setString(2, airports[2]);
					prepStmt16.setString(3, flightSeat2);
					prepStmt16.setInt(4, 2);
					prepStmt16.executeUpdate();
				

					//insert into the reservation_flight_seat_info table
					//(confirmation_number,itiernary, seat_id_booked, trip_order)
					String queryForResFltInfo3 = "INSERT INTO reservation_flight_seat_info (confirmation_number,itiernary, seat_id_booked, trip_order) values (?, ?, ?, ?);";
					//System.out.println(queryForResFltInfo );
					prepStmt17 = conn.prepareStatement(queryForResFltInfo3);
					prepStmt17.setString(1, reserId);
					prepStmt17.setString(2, airports[2]);
					prepStmt17.setString(3, flightSeat3);
					prepStmt17.setInt(4, 3);
					prepStmt17.executeUpdate();
					
					String queryGetAircraftID1 = "select A.aircraftid from tempavail A where A.availid = "+avail1+";"; 
					prepStmt18 = conn.prepareStatement(queryGetAircraftID1);
					resSet18 = prepStmt18.executeQuery();
					resSet18.next();
					String planeID1 = resSet18.getString(1);

					String queryGetAircraftID2 = "select A.aircraftid from tempavail A where A.availid = "+avail2+";"; 
					prepStmt19 = conn.prepareStatement(queryGetAircraftID2);
					resSet19 = prepStmt19.executeQuery();
					resSet19.next();
					String planeID2 = resSet19.getString(1); 

					String queryGetAircraftID3 = "select A.aircraftid from tempavail A where A.availid = "+avail2+";"; 
					prepStmt20 = conn.prepareStatement(queryGetAircraftID3);
					resSet20 = prepStmt20.executeQuery();
					resSet20.next();
					String planeID3 = resSet20.getString(1);
					
					String queryInsertTempBooked1 = "INSERT INTO tempBookedSeats (availId, passengerID, reservationID, flightDate, flightID, "
							+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
					prepStmt21 = conn.prepareStatement(queryInsertTempBooked1);
					prepStmt21.setInt(1, avail1);
					prepStmt21.setString(2, passengerID);
					prepStmt21.setString(3, reserId);
					prepStmt21.setDate(4, TripDate);
					prepStmt21.setString(5, flightsIDs[0]);
					prepStmt21.setString(6, flightSeat1);
					prepStmt21.setString(7, seatType);
					prepStmt21.setString(8, planeID1);
					prepStmt21.setString(9, connection1[0]);
					prepStmt21.setString(10, connection1[1]);	                
					prepStmt21.executeUpdate();
					
					String queryInsertTempBooked2 = "INSERT INTO tempBookedSeats (availId, passengerID, reservationID, flightDate, flightID, "
							+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
					prepStmt22 = conn.prepareStatement(queryInsertTempBooked2);
					prepStmt22.setInt(1, avail2);
					prepStmt22.setString(2, passengerID);
					prepStmt22.setString(3, reserId);
					prepStmt22.setDate(4, TripDate);
					prepStmt22.setString(5, flightsIDs[1]);
					prepStmt22.setString(6, flightSeat2);
					prepStmt22.setString(7, seatType);
					prepStmt22.setString(8, planeID2);
					prepStmt22.setString(9, connection2[0]);
					prepStmt22.setString(10, connection2[1]);	                
					prepStmt22.executeUpdate();
					
					String queryInsertTempBooked3 = "INSERT INTO tempBookedSeats (availId, passengerID, reservationID, flightDate, flightID, "
							+ "seatNum, seatClass, aircraftID, origin, destination) values (?,?,?,?,?,?,?,?,?,?)";
					prepStmt23 = conn.prepareStatement(queryInsertTempBooked3);
					prepStmt23.setInt(1, avail3);
					prepStmt23.setString(2, passengerID);
					prepStmt23.setString(3, reserId);
					prepStmt23.setDate(4, TripDate);
					prepStmt23.setString(5, flightsIDs[2]);
					prepStmt23.setString(6, flightSeat3);
					prepStmt23.setString(7, seatType);
					prepStmt23.setString(8, planeID3);
					prepStmt23.setString(9, connection3[0]);
					prepStmt23.setString(10, connection3[1]);	                
					prepStmt23.executeUpdate();
					
					String queryDelFromAvail1 = "DELETE from tempavail A WHERE A.availId = ?";
					prepStmt24 = conn.prepareStatement(queryDelFromAvail1);
					prepStmt24.setInt(1, avail1);
					prepStmt24.executeUpdate();
				
					String queryDelFromAvail2 = "DELETE from tempavail A WHERE A.availId = ?";
					prepStmt25 = conn.prepareStatement(queryDelFromAvail2);
					prepStmt25.setInt(1, avail2);
					prepStmt25.executeUpdate();

					String queryDelFromAvail3 = "DELETE from tempavail A WHERE A.availId = ?";
					prepStmt26 = conn.prepareStatement(queryDelFromAvail3);
					prepStmt26.setInt(1, avail3);
					prepStmt26.executeUpdate();

					conn.commit();
					System.out.println("Booked seat number "+flightSeat1+" with reservation ID "+reserId+" on flight "+connection1[0]+" to "+connection1[1]+",\n" +"seat number "
					+flightSeat2 +" on flight "+connection2[0]+" to "+connection2[1]+", \n"+"And booked seat number "+flightSeat3+" on flight "+connection3[0]+" to "+connection3[1]+" for a two stop flight between "+connection1[0]+" and "+connection3[1]);
					System.out.println();
	            
	            }
			}
		}catch(Exception ex){
			try {
				System.err.println(ex.getMessage());
				conn.rollback();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}

}

