"""
ingestion — Smart Parking Occupancy Analytics
=============================================
Ingestion module for the Smart Parking Occupancy Analytics project
(AI372IA · RV College of Engineering, Bengaluru).

Sub-modules
-----------
schema              Canonical event schema (JSON-Schema, PySpark StructType, InfluxDB mapping).
validator           Field-level validation and dead-letter-queue record construction.
producer            KafkaProducer wrapper for parking events and DLQ events.
historical_replay   Historical dataset replay at configurable throughput rates.
"""
