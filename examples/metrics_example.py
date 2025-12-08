#!/usr/bin/env python3
"""Example script demonstrating Prometheus metrics HTTP endpoint."""

import requests


def fetch_metrics():
    """Fetch and display Prometheus metrics."""
    
    print("=" * 60)
    print("Kryten User Statistics - Prometheus Metrics")
    print("=" * 60)
    
    try:
        response = requests.get("http://localhost:28282/metrics")
        
        if response.status_code == 200:
            print("\nMetrics (raw Prometheus format):")
            print(response.text)
            
            # Parse and display in human-readable format
            print("\n" + "=" * 60)
            print("Parsed Metrics:")
            print("=" * 60)
            
            lines = response.text.split('\n')
            current_metric = None
            
            for line in lines:
                if line.startswith('#'):
                    if 'HELP' in line:
                        # Extract metric name and description
                        parts = line.split(' ', 3)
                        if len(parts) >= 4:
                            current_metric = parts[2]
                            print(f"\n{current_metric}:")
                            print(f"  Description: {parts[3]}")
                elif line.strip() and current_metric:
                    # Extract value
                    parts = line.split()
                    if len(parts) >= 2:
                        value = parts[-1]
                        print(f"  Value: {value}")
                        
        else:
            print(f"Error: HTTP {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("\nError: Could not connect to metrics server")
        print("\nMake sure:")
        print("  1. kryten-userstats is running")
        print("  2. Metrics server is enabled (port 28282)")
    except Exception as e:
        print(f"\nError: {e}")


if __name__ == "__main__":
    fetch_metrics()
