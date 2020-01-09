package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.events.LargeTripsEvent;
import master2019.flink.YellowTaxiTrip.events.TripEvent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * In this class the Large trips program has to be implemented
 */

public class LargeTrips {

    private static final int MIN_TIME_IN_MILLISECONDS = 20 * 60 * 1000 ;

    public static final String LARGE_TRIPS_FILE = "largeTrips.csv";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> rowsSource = env.readTextFile(params.get("input"));
        SingleOutputStreamOperator<TripEvent> mappedRows = rowsSource.map(new YellowTaxiTrip.Tokenizer());

        LargeTrips.run(mappedRows)
                .writeAsCsv(String.format("%s/%s",params.get("output"),LARGE_TRIPS_FILE),org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Large Trips");
    }

    public static SingleOutputStreamOperator<LargeTripsEvent> run(SingleOutputStreamOperator<TripEvent> stream) {
        return stream
                .filter((TripEvent e) -> ((e.get_tpep_dropoff_datetime().getTime() - e.get_tpep_pickup_datetime().getTime())  >= MIN_TIME_IN_MILLISECONDS))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TripEvent>() {
                    @Override
                    public long extractAscendingTimestamp(TripEvent tripEvent) {
                        return (tripEvent.get_tpep_pickup_datetime().getTime());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(3)))
                .apply(new LargeTrips.LargeTripsWindow());
    }

    // private static class JFKAlarmKey extends Tuple2<Integer,Long>{}

    private static class LargeTripsWindow implements WindowFunction<TripEvent, LargeTripsEvent, Tuple, TimeWindow> {

        private LargeTripsEvent largeTripsEvent = new LargeTripsEvent();

        @Override
        public void apply(Tuple key, TimeWindow timeWindow,
                          Iterable<TripEvent> iterable,
                          Collector<LargeTripsEvent> collector) throws Exception {

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

            Date pickupParsedDate = dateFormat.parse("9999-00-0 00:00:00");
            Timestamp pickupTimestamp = new java.sql.Timestamp(pickupParsedDate.getTime());

            Date dropoffParsedDate = dateFormat.parse("0000-00-0 00:00:00");
            Timestamp dropoffTimestamp = new java.sql.Timestamp(dropoffParsedDate.getTime());

            int vendor_ID = 0;
            int trips_count = 0;

            for (TripEvent e : iterable) {

                vendor_ID = e.f0;

                Timestamp currentPickupTimestamp = e.f1;
                Timestamp currentDropoffTimestamp = e.f2;

                if(currentPickupTimestamp.before(pickupTimestamp))
                {
                    pickupTimestamp = currentPickupTimestamp;
                }
                if(currentDropoffTimestamp.after(dropoffTimestamp))
                {
                    dropoffTimestamp = currentDropoffTimestamp;
                }

                trips_count = trips_count +1;
            }

            if(trips_count>=5)
            {
                largeTripsEvent.set_VendorID(vendor_ID);
                largeTripsEvent.set_day(pickupTimestamp.toString().substring(0,10));
                largeTripsEvent.set_numberOfTrips(trips_count);
                largeTripsEvent.set_tpep_pickup_datetime(pickupTimestamp);
                largeTripsEvent.set_tpep_dropoff_datetime(dropoffTimestamp);
                collector.collect(largeTripsEvent);
            }
        }
    }
}
