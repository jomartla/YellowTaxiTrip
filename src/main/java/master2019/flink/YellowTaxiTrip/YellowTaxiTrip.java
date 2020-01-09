package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.events.TripEvent;
import org.apache.flink.api.common.functions.MapFunction;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class YellowTaxiTrip {

    public static final class Tokenizer implements MapFunction<String, TripEvent> {

        TripEvent event = new TripEvent();

        @Override
        public TripEvent map(String s) throws Exception {

            String[] fields = s.split(",");

            Object[] fieldsObject = Arrays.stream(fields).toArray();

            event.set_VendorID(Integer.parseInt(fieldsObject[0].toString()));

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

            Date parsedDate = dateFormat.parse(fieldsObject[1].toString());
            event.set_tpep_pickup_datetime( new java.sql.Timestamp(parsedDate.getTime()));

            parsedDate = dateFormat.parse(fieldsObject[2].toString());
            event.set_tpep_dropoff_datetime( new java.sql.Timestamp(parsedDate.getTime()));

            event.set_passenger_count(Integer.parseInt(fieldsObject[3].toString()));
            event.set_trip_distance(Float.parseFloat(fieldsObject[4].toString()));
            event.set_RatecodeID(Integer.parseInt(fieldsObject[5].toString()));
            event.set_store_and_fwd_fla(fieldsObject[6].toString());
            event.set_PULocationID(Integer.parseInt(fieldsObject[7].toString()));
            event.set_DOLocationID(Integer.parseInt(fieldsObject[8].toString()));
            event.set_payment_type(Integer.parseInt(fieldsObject[9].toString()));
            event.set_fare_amount(Float.parseFloat(fieldsObject[10].toString()));
            event.set_Extra(Float.parseFloat(fieldsObject[11].toString()));
            event.set_mta_tax(Float.parseFloat(fieldsObject[12].toString()));
            event.set_tip_amount(Float.parseFloat(fieldsObject[13].toString()));
            event.set_tolls_amount(Float.parseFloat(fieldsObject[14].toString()));
            event.set_improvement_surcharge(Float.parseFloat(fieldsObject[15].toString()));
            event.set_total_amount(Float.parseFloat(fieldsObject[16].toString()));
            event.set_congestion_surcharge(Float.parseFloat(fieldsObject[17].toString()));

            return event;
        }
    }
}
