package master2019.flink.YellowTaxiTrip.events;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import java.sql.Timestamp;

public class LargeTripsEvent extends Tuple5<Integer,String,Integer,Timestamp,Timestamp> {

    public LargeTripsEvent() {

    }

    // Setters
    public void set_VendorID(int VendorID) {
        f0 = VendorID;
    }

    public void set_day(String day) {
        f1 = day;
    }

    public void set_numberOfTrips(int numberOfTrips) {
        f2 = numberOfTrips;
    }

    public void set_tpep_pickup_datetime(Timestamp tpep_pickup_datetime) {
        f3 = tpep_pickup_datetime;
    }

    public void set_tpep_dropoff_datetime(Timestamp get_tpep_dropoff_datetime) {
        f4 = get_tpep_dropoff_datetime;
    }
}
