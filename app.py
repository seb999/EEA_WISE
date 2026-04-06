#!/usr/bin/env python3
"""
Entry point to start the EEA Groundwater Data API service.
"""

from src.api_server import start_server

if __name__ == "__main__":
    print("Starting EEA Groundwater Data API service...")
    print("Server will be available at: http://127.0.0.1:8081")
    print("API documentation at: http://127.0.0.1:8081/docs")
    print("Interactive API at: http://127.0.0.1:8081/redoc")
    
    start_server()