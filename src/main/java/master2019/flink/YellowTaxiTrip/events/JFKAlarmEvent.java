package master2019.flink.YellowTaxiTrip.events;

import java.sql.Timestamp;
import org.apache.flink.api.java.tuple.Tuple4;

public class JFKAlarmEvent extends Tuple4<Integer, Timestamp, Timestamp, Integer> {

    // Empty constructor
    public JFKAlarmEvent() {

    }

    // Setters
    public void set_VendorID(int VendorID) {
        f0 = VendorID;
    }

    public void set_tpep_pickup_datetime(Timestamp tpep_pickup_datetime) {
        f1 = tpep_pickup_datetime;
    }

    public void set_tpep_dropoff_datetime(Timestamp get_tpep_dropoff_datetime) {
        f2 = get_tpep_dropoff_datetime;
    }

    public void set_passenger_count(int get_passenger_count) {
        f3 = get_passenger_count;
    }
}

