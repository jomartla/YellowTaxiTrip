# YellowTaxiTrip

## Introduction
New York Taxi and Limousine Commission stores all trips done by yellow and green taxis. This
data is reported by each taxi and is sent to a data center that processes this information in order to detect
irregular situations.

## Data
Each taxi reports all the information regarding each trip with the following format: 
* **VendorID**: A code indicating the TPEP provider that provided the record. 1 = Creative Mobile Technologies,
LLC; 2 = VeriFone Inc…
* **tpep_pickup_datetime**: The date and time when the meter was engaged.
* **tpep_dropoff_datetime**: The date and time when the meter was disengaged.
* **passenger_count**: The number of passengers in the vehicle. This is a driver-entered value.
* **trip_distance**: The elapsed trip distance in miles reported by the taximeter.
* **RatecodeID**: The final rate code in effect at the end of the trip. 1 = Standard rate; 2 = JFK; 3 = Newark; 4 =
Nassau or Westchester; 5 = Negotiated fare; 6 = Group ride.
* **store_and_fwd_flag**: This flag indicates whether the trip record was held in vehicle memory before
sending it to the vendor, aka “store and forward,” because the vehicle did not have a connection to the
server. Y= store and forward trip; N= not a store and forward trip.
* **PULocationID**: TLC Taxi Zone in which the taximeter was engaged.
* **DOLocationID**: TLC Taxi Zone in which the taximeter was disengaged.
* **payment_type**: A numeric code signifying how the passenger paid for the trip. 1 = Credit card; 2 = Cash;
3 = No charge; 4 = Dispute; 5 = Unknown; 6 = Voided trip.
* **fare_amount**: The time-and-distance fare calculated by the meter.
* **Extra**: Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and
overnight charges.
* **mta_tax**: $0.50 MTA tax that is automatically triggered based on the metered rate in use.
* **tip_amount**: Tip amount – This field is automatically populated for credit card tips. Cash tips are not
included.
* **tolls_amount**: Total amount of all tolls paid in trip.
* **improvement_surcharge**: $0.30 improvement surcharge assessed trips at the flag drop. The
improvement surcharge began being levied in 2015.
* **total_amount**: The total amount charged to passengers. It does not include cash tips.
* **congestion_surcharge**: The surcharge applied when the trips goes through a congested area.

## Objective
The goal of this project is to develop a Java program using Flink for implementing the following
functionality:
1. **JFK airport trips**: It informs about the trips ending at JFK airport with two or more passengers
each hour for each vendorID. The output must have the following format: vendorID,
tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count. Being
tpep_dropoff_datetime the time the last trip finishes, tpep_pickup_datetime the starting
time of the first trip and passenger_count the total number of passengers.
2. **Large trips**. It reports the vendors that do 5 or more trips during 3 hours that take at least 20
minutes. The output has the following format: VendorID, day, numberOfTrips,
tpep_pickup_datetime, tpep_dropoff_datetime, being tpep_dropoff_datetime the time the
last trip finishes and tpep_pickup_datetime the starting time of the first trip.
