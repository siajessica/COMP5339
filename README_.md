# Fuel Check Data Augmentation Pipeline

This project integrates multiple data sources to create a unified, enriched dataset of fuel stations across New South Wales, Australia. It provides enhanced insight into fuel prices, station locations, and brand identities using public data and external APIs.

---

## Data Sources Integrated

### 1. Fuel Check Historical Data (Web Crawling)
- **Source**: [NSW Open Data Portal](https://data.nsw.gov.au/data/dataset/fuel-check)
- **Details**: Crawled CSV datasets for **2024** and **2025** with historical fuel prices and transactions.

### 2. Fuel Station Details (Live API)
- **API Endpoint**: `https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices`
- **Details**:
  - Live fuel station metadata
  - GPS coordinates (latitude & longitude)
  - Brand, fuel types, AdBlue availability, etc.

### 3. Brand Logos (Brandfetch API)
- **API**: `https://api.brandfetch.io/v2/`
- **Function**: Pulls logo images for popular fuel brands like Ampol, Shell, BP, 7-Eleven, etc.
- ⚠️ Some logos may be missing or mismatched. These are flagged for **manual review**.

---

## ✅ Integration Workflow

### 1. Setup Python Environment
```bash
python -m venv .venv
source .venv/bin/activate 
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Pipeline
```bash
python app.py
```

## Outputs

The pipeline produces the following files and directories after execution:

| File / Folder             | Description                                                         |
|---------------------------|---------------------------------------------------------------------|
| `data/fuel_price.csv`       | Raw fuel price data aggregated from crawling and API integration    |
| `data/station_detail.csv` | Fuel station metadata with GPS coordinates and AdBlue availability |
| `data/all.csv`           | Final merged dataset combining prices, stations, and branding info |
| `src/`            | Directory containing downloaded brand logo images                  |