package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.events.JFKAlarmEvent;
import master2019.flink.YellowTaxiTrip.events.TripEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {

    private static final int MIN_PASSENGER_COUNT = 2;

    public static SingleOutputStreamOperator<JFKAlarmEvent> run(SingleOutputStreamOperator<TripEvent> stream) {
        return stream
                .filter((TripEvent e) -> (e.get_passenger_count() >= MIN_PASSENGER_COUNT) && (e.get_RatecodeID()==2))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TripEvent>() {
                    @Override
                    public long extractAscendingTimestamp(TripEvent tripEvent) {
                        return tripEvent.get_tpep_dropoff_datetime().getTime()*1000;
                    }
                })
                .keyBy(new KeySelector<TripEvent, JFKAlarmKey>() {

                    JFKAlarmKey key = new JFKAlarmKey();

                    @Override
                    public JFKAlarmKey getKey(TripEvent tripEvent) throws Exception {
                        key.f0 = tripEvent.get_VendorID();
                        key.f1 = 10000000 * (tripEvent.get_tpep_dropoff_datetime().getTime()/10000000);
                        return key;
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(new JFKAlarmWindow());

    }

    private static class JFKAlarmKey extends Tuple2<Integer,Long>{}

    private static class JFKAlarmWindow implements WindowFunction<TripEvent, JFKAlarmEvent, JFKAlarmKey, TimeWindow> {

        private JFKAlarmEvent jfkAlarmEvent = new JFKAlarmEvent();

        @Override
        public void apply(JFKAlarmKey key, TimeWindow timeWindow,
                          Iterable<TripEvent> iterable,
                          Collector<JFKAlarmEvent> collector) throws Exception {

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

            Date pickupParsedDate = dateFormat.parse("9999-00-0 00:00:00");
            Timestamp pickupTimestamp = new java.sql.Timestamp(pickupParsedDate.getTime());

            Date dropoffParsedDate = dateFormat.parse("0000-00-0 00:00:00");
            Timestamp dropoffTimestamp = new java.sql.Timestamp(dropoffParsedDate.getTime());

            int passenger_count = 0;

            for (TripEvent e : iterable) {
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

                passenger_count = passenger_count + e.f3;
            }

            jfkAlarmEvent.set_VendorID(key.f0);
            jfkAlarmEvent.set_tpep_pickup_datetime(pickupTimestamp);
            jfkAlarmEvent.set_tpep_dropoff_datetime(dropoffTimestamp);
            jfkAlarmEvent.set_passenger_count(passenger_count);
            collector.collect(jfkAlarmEvent);
        }
    }
}
