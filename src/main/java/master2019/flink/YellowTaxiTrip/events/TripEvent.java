package master2019.flink.YellowTaxiTrip.events;

import org.apache.flink.api.java.tuple.Tuple18;

import java.sql.Timestamp;

public class TripEvent extends Tuple18<Integer, Timestamp,Timestamp,Integer,Float,Integer,String,Integer,Integer,Integer, Float,Float,Float,Float,Float,Float,Float,Float> {

    // Empty constructor
    public TripEvent() {

    }

    // Getters
    public int get_VendorID() {
        return f0;
    }

    public Timestamp get_tpep_pickup_datetime() {
        return f1;
    }

    public Timestamp get_tpep_dropoff_datetime() {
        return f2;
    }

    public Integer get_passenger_count() {
        return f3;
    }

    public Float get_trip_distance() {
        return f4;
    }

    public Integer get_RatecodeID() {
        return f5;
    }

    public String get_store_and_fwd_fla() {
        return f6;
    }

    public Integer get_PULocationID() {
        return f7;
    }

    public Integer get_DOLocationID() {
        return f8;
    }

    public Integer get_payment_type() {
        return f9;
    }

    public Float get_fare_amount() {
        return f10;
    }

    public Float get_Extra() {
        return f11;
    }

    public Float get_mta_tax() {
        return f12;
    }

    public Float get_tip_amount() {
        return f13;
    }

    public Float get_tolls_amount() {
        return f14;
    }

    public Float get_improvement_surcharge() {
        return f15;
    }

    public Float get_total_amount() {
        return f16;
    }

    public Float get_congestion_surcharge() {
        return f17;
    }

    // Setters

    public void set_VendorID(int VendorID) {
        f0 = VendorID;
    }

    public void set_tpep_pickup_datetime(Timestamp pep_pickup_datetime) {
        f1 = pep_pickup_datetime;
    }

    public void set_tpep_dropoff_datetime(Timestamp tpep_dropoff_datetime) {
        f2 = tpep_dropoff_datetime;
    }

    public void set_passenger_count(int passenger_count) {
        f3 = passenger_count;
    }

    public void set_trip_distance(Float trip_distance) {
        f4 = trip_distance;
    }

    public void set_RatecodeID(int RatecodeID) {
        f5 = RatecodeID;
    }

    public void set_store_and_fwd_fla(String store_and_fwd_fla) {
        f6 = store_and_fwd_fla;
    }

    public void set_PULocationID(int PULocationID) {
        f7 = PULocationID;
    }

    public void set_DOLocationID(int DOLocationID) {
        f8 = DOLocationID;
    }

    public void set_payment_type(int payment_type) {
        f9 = payment_type;
    }

    public void set_fare_amount(float fare_amount) {
        f10 = fare_amount;
    }

    public void set_Extra( float Extra) {
        f11 = Extra;
    }

    public void set_mta_tax(float mta_tax) {
        f12 = mta_tax;
    }

    public void set_tip_amount(float tip_amount) {
        f13 = tip_amount;
    }

    public void set_tolls_amount(float tolls_amount) {
        f14 = tolls_amount;
    }

    public void set_improvement_surcharge(float improvement_surcharge) {
        f15 = improvement_surcharge;
    }

    public void set_total_amount(float total_amount) {
        f16 = total_amount;
    }

    public void set_congestion_surcharge(float congestion_surcharge) {
        f17 = congestion_surcharge;
    }
}

