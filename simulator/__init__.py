"""
simulator — Smart Parking Occupancy Analytics
=============================================
Simulation module for the Smart Parking Occupancy Analytics project
(AI372IA · RV College of Engineering, Bengaluru).

This module produces a realistic, configurable stream of parking-sensor
events that can be published to Kafka or consumed in dry-run mode.

Sub-modules
-----------
parking_lot         ParkingLot / ParkingSlot domain model with slot-state FSM.
traffic_model       Time-of-day traffic patterns and arrival/departure probabilities.
fault_injector      Probabilistic fault injection (duplicates, late events, invalids).
generator           Core event generator; maintains lot state and emits event dicts.
sensor              SensorNetwork wrapper that enriches events with IoT metadata.
main                CLI entry point (``python -m simulator.main``).
"""
