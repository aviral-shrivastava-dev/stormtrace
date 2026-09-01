"""StormTrace test suite.

Tests import modules from src/ directly (see conftest.py, which puts src
on sys.path). No test contacts CelesTrak, NOAA, or SatNOGS: network calls
belong to the collectors, and public usage policies must be respected
from any machine, including CI runners.
"""
