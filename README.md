# C3ntinel Automation

This project automates downloading meter data from the C3ntinel API, processes it with MDT/CDD calculations, and uploads the results as a CSV file to Google Drive (or other cloud storage).

## Features

- Authenticates with C3ntinel API using client credentials
- Fetches meter metadata and readings for a date range
- Retrieves temperature data to calculate MDT and CDD values
- Outputs data to CSV file
- Uploads the CSV to a specific Google Drive folder
- Designed to be scheduled to run automatically (e.g. with Windows Task Scheduler)

## Prerequisites

- Python 3.8+
- Google API credentials JSON (`credentials.json`) for Drive API
- C3ntinel client ID and secret

## Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/c3ntinel-automation.git
   cd c3ntinel-automation
