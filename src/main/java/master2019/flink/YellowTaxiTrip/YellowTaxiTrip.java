package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.events.TripEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.Arrays;

public class YellowTaxiTrip {

    public static final String JFK_ALARMS_FILE = "jfkAlarms.csv";
    public static final String LARGE_TRIPS_FILE = "largeTrips.csv";

    public static void main(String[] args){

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> rowsSource = env.readTextFile(params.get("input"));
        SingleOutputStreamOperator<TripEvent> mappedRows = rowsSource.map(new Tokenizer());


    }

    public static final class Tokenizer implements MapFunction<String, TripEvent> {

        private static final long serialVersionUID = 1L;

        TripEvent event = new TripEvent();

        @Override
        public TripEvent map(String s) throws Exception {

            String[] fields = s.split(",");

            Object[] fieldsObject = Arrays.stream(fields).toArray();

            event.set_VendorID((int) fieldsObject[0]);
            event.set_tpep_pickup_datetime((Timestamp) fieldsObject[1]);
            event.set_tpep_dropoff_datetime((Timestamp) fieldsObject[2]);
            event.set_passenger_count((int) fieldsObject[3]);
            event.set_trip_distance((float) fieldsObject[4]);
            event.set_RatecodeID((int) fieldsObject[5]);
            event.set_store_and_fwd_fla((String) fieldsObject[6]);
            event.set_PULocationID((int) fieldsObject[7]);
            event.set_DOLocationID((int) fieldsObject[8]);
            event.set_payment_type((int) fieldsObject[9]);
            event.set_fare_amount((float) fieldsObject[10]);
            event.set_Extra((float) fieldsObject[11] );
            event.set_mta_tax((float) fieldsObject[12]);
            event.set_tip_amount((float) fieldsObject[13]);
            event.set_tolls_amount((float) fieldsObject[14]);
            event.set_improvement_surcharge((float) fieldsObject[15]);
            event.set_total_amount((float) fieldsObject[16]);
            event.set_congestion_surcharge((float) fieldsObject[17]);

            return event;
        }
    }
}
