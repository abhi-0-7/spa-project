# Known Limitations & Academic Disclaimers
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Single-Laptop Development Deployment Constraints

To respect the non-negotiable requirement of operating on a single laptop without cloud or cluster infrastructure, the following architectural choices were made:

### 1.1 Single Kafka Broker (Replication Factor = 1)
- **Limitation:** Apache Kafka runs as a single KRaft broker container.
- **Academic Context:** High availability (HA) and broker partition leader failover cannot be demonstrated without at least 3 distinct brokers, which would require an additional 1.5 GB of RAM.
- **Impact:** If the single Kafka container terminates, the broker is down. However, committed disk logs persist and resume upon restart.

### 1.2 Spark Local Mode (`local[*]`)
- **Limitation:** PySpark Structured Streaming runs in `local[*]` mode rather than on a multi-node standalone or YARN cluster.
- **Academic Context:** Local mode executes the exact same Structured Streaming APIs, query plans, state stores, and micro-batch engine as a distributed cluster, while conserving memory. Worker node failover is not simulated.

### 1.3 Synthetic Sensor Simulation
- **Limitation:** Physical IoT sensors, ultrasonic rangefinders, and microcontrollers are not used.
- **Academic Context:** The system uses a virtual IoT sensor network simulator (`simulator/`). Every event is clearly tagged as `"source": "SIMULATED_SENSOR"`. The project demonstrates Stream Processing and Analytics concepts, not hardware electronics.

---

## 2. Dataset Attribution and Limitations

### 2.1 Bengaluru OpenCity.in Parking Data
- **Limitation:** OpenCity.in provides static geospatial polygon coordinates for parking locations across Bengaluru, but does **not** provide live real-time sensor streams.
- **Assumption:** Lot capacities (e.g. 120, 80, 200) are assumed academic parameters for modeling.

### 2.2 Historical Replay Data
- **Limitation:** City of Melbourne 2019 dataset represents historical data from an Australian municipality, not live Indian parking slots.
- **Separation:** When replaying Melbourne data, events are stamped with `"city": "Melbourne"` and `"source": "HISTORICAL_REPLAY"` to maintain strict scientific integrity.
