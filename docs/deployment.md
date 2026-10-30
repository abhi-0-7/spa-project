# Local Deployment & Execution Guide
## Smart Parking Occupancy Analytics
**Course:** AI372IA — Stream Processing and Analytics, RV College of Engineering  

---

## 1. Prerequisites Checklist

Before launching the project, verify that the following local software components are installed:
- [x] **Windows 10 / 11 (64-bit)**
- [x] **Python 3.10 to 3.13** (Tested with Python 3.13.1)
- [x] **Java OpenJDK 8, 11, or 17** (Detected at `C:\Program Files\Eclipse Adoptium\jdk-8.0.492.9-hotspot`)
- [x] **Docker Desktop 4.x** (Ensuring Docker daemon is running)

---

## 2. Step-by-Step Execution Workflow

### Step 1: Open PowerShell / Command Prompt
Navigate to the project root directory:
```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
```

### Step 2: Configure Environment Variables
Set the `JAVA_HOME` variable for the current terminal session:
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-8.0.492.9-hotspot"
```

### Step 3: Run Automated Environment Verification
```powershell
python scripts/setup.py
```
*This validates dependencies, directory paths, and creates `.env` configuration.*

### Step 4: Start Infrastructure Containers
Start Apache Kafka (KRaft mode), InfluxDB 2.7, and Grafana 10.4:
```powershell
docker compose up -d
```
Verify container health:
```powershell
python scripts/health_check.py
```

### Step 5: Start the Virtual Sensor Network Simulator (Terminal 1)
Start generating real-time parking sensor events:
```powershell
# Normal mode (50 events/sec, diurnal traffic patterns)
python -m simulator.main --mode normal --rate 50
```

### Step 6: Start Spark Structured Streaming Engine (Terminal 2)
In a second terminal window:
```powershell
cd C:\Users\Abhishek\Downloads\spa\smart-parking-analytics
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-8.0.492.9-hotspot"
python -m streaming.main --watermark "2 minutes" --trigger "10 seconds"
```

### Step 7: Open the Real-Time Grafana Dashboard
Open your web browser and navigate to:
- **URL:** [http://localhost:3000](http://localhost:3000)
- **Username:** `admin`
- **Password:** `parking123`
- **Dashboard:** Select **Smart Parking Occupancy Analytics — Bengaluru** (pre-provisioned with 24 live panels).

---

## 3. Web Service Directory

| Service | Local URL | Default Credentials | Description |
|---|---|---|---|
| **Grafana** | `http://localhost:3000` | `admin` / `parking123` | 24-panel real-time operational dashboard |
| **InfluxDB** | `http://localhost:8086` | `admin` / `parking123admin` | Time-series data explorer & bucket manager |
| **Spark UI** | `http://localhost:4040` | (No authentication required) | Live DAG execution, stage timers, query stats |
| **Kafka Broker** | `localhost:9092` | (PLAINTEXT) | KRaft event broker external endpoint |

---

## 4. Teardown Instructions

To gracefully stop the pipeline:
1. Press `Ctrl+C` in the Simulator and Spark terminals.
2. Stop the Docker Compose stack:
   ```powershell
   docker compose down
   ```
3. To reset checkpoints and start from a clean slate:
   ```powershell
   python scripts/reset.py --force
   ```
