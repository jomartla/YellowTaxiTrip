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

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
/*
    private static final int MIN_PASSENGER_COUNT = 2;

    public static SingleOutputStreamOperator<JFKAlarmEvent> run(SingleOutputStreamOperator<TripEvent> stream) {
        return stream
                .filter((TripEvent e) -> (e.get_passenger_count() >= MIN_PASSENGER_COUNT) && (e.get_RatecodeID()==2))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TripEvent>() {
                    @Override
                    public long extractAscendingTimestamp(TripEvent tripEvent) {
                        return tripEvent.get_tpep_dropoff_datetime().getTime();
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

        private JFKAlarmEvent avgSpeedEvent = new JFKAlarmEvent();

        @Override
        public void apply(JFKAlarmKey key, TimeWindow timeWindow,
                          Iterable<TripEvent> iterable,
                          Collector<JFKAlarmEvent> collector) throws Exception {

            Timestamp pickupDatetime = Timestamp.valueOf("2007-09-23 10:10:10.0");
            Timestamp dropoffDatetime = new Timestamp();


            int pos1 = Integer.MAX_VALUE;

            int time2 = 0;
            int pos2 = 0;

            for (TripEvent e : iterable) {
                int currentTime = e.f0;
                int currentPos = e.f7;
                completedSegments |= 1 << (56 - e.f6);
                time1 = Math.min(time1, currentTime);
                pos1 = Math.min(pos1, currentPos);
                time2 = Math.max(time2, currentTime);
                pos2 = Math.max(pos2, currentPos);
            }

            boolean completed = false;
            if (completedSegments == 0b11111)
                completed = true;

            if (completed) {
                double avgSpeed = (pos2 - pos1) * 1.0 / (time2 - time1) * 2.23694;
                if (avgSpeed > 60) {
                    avgSpeedEvent.setEntryTime(time1);
                    avgSpeedEvent.setExitTime(time2);
                    avgSpeedEvent.setVid(key.f0);
                    avgSpeedEvent.setHighway(key.f1);
                    avgSpeedEvent.setDirection(key.f2);
                    avgSpeedEvent.setAvgSpeed(avgSpeed);
                    collector.collect(avgSpeedEvent);
                }
            }
        }
    }

 */
}
