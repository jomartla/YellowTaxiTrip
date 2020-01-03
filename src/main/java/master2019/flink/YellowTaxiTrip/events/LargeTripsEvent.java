package master2019.flink.YellowTaxiTrip.events;

import org.apache.flink.api.java.tuple.Tuple3;

public class LargeTripsEvent extends Tuple3<Integer,Integer,Integer> {

    public LargeTripsEvent() {

    }

    // Setters
    public void set_VendorID(int VendorID) {
        f0 = VendorID;
    }

    public void set_day(int day) {
        f1 = day;
    }

    public void set_numberOfTrips(int numberOfTrips) {
        f2 = numberOfTrips;
    }
}
