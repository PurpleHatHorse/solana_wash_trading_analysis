# Solana Wash Trading Analysis

A comprehensive Python-based system for detecting wash trading patterns in Solana token transfers using the Arkham Intelligence API.

## 🎯 Features

- **Data Collection**: Automated fetching from Arkham Intelligence API with pagination
- **Data Processing**: Structured parsing of blockchain transfer data
- **Wash Trading Detection**: Multiple detection algorithms:
  - Circular trading patterns
  - Rapid round-trip trades
  - Self-transfers
  - Coordinated wallet clusters
  - Volume concentration analysis
  - Timing pattern analysis
  - High-frequency address pairs
- **Visualization**: Network graphs, timelines, and statistical charts
- **Reporting**: Comprehensive text reports and CSV exports

## 📋 Prerequisites

- Python 3.8+
- Arkham Intelligence API key ([Get one here](https://platform.arkhamintelligence.com/))
- pip

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/solana-wash-trading-analysis.git
cd solana-wash-trading-analysis
```

### 2. Setup Environment
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment

Create `.env` file:
```env
ARKHAM_API_KEY=your_api_key_here
TARGET_TOKEN=EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm
TOKEN_SYMBOL=WIF
CHAIN=solana
TIME_RANGE=30d
MIN_USD_VALUE=100
MAX_TRANSFERS=5000
```

### 4. Run Analysis
```bash
# Step 1: Collect data
python scripts/collect_data.py

# Step 2: Process data
python scripts/process_data.py WIF_solana_30d_YYYYMMDD_HHMMSS.json

# Step 3: Detect wash trading
python analysis/detect_wash_trading.py WIF_solana_30d_YYYYMMDD_HHMMSS_processed.csv

# Step 4: Generate visualizations
python analysis/visualize_results.py WIF_solana_30d_YYYYMMDD_HHMMSS_processed.csv
```

## 📁 Project Structure
```
solana-wash-trading-analysis/
├── data/
│   ├── raw/              # Raw JSON data from API
│   └── processed/        # Processed CSV files
├── scripts/
│   ├── arkham_client.py  # API client
│   ├── collect_data.py   # Data collection
│   └── process_data.py   # Data processing
├── analysis/
│   ├── detect_wash_trading.py  # Detection algorithms
│   └── visualize_results.py    # Visualization
├── outputs/
│   ├── reports/          # Analysis reports
│   └── visualizations/   # Charts and graphs
├── requirements.txt
├── .env
└── README.md
```

## 🔍 Detection Methods

### 1. Circular Trading
Detects cycles where tokens flow A→B→C→A, indicating coordinated wash trading.

### 2. Rapid Round-Trips
Identifies address pairs that quickly trade back and forth (< 24 hours).

### 3. Self-Transfers
Flags addresses sending tokens to themselves.

### 4. Wallet Clusters
Detects groups of wallets acting in coordination.

### 5. Volume Concentration
Analyzes if small number of addresses control majority of volume.

### 6. Timing Patterns
Identifies unusual temporal patterns in trading activity.

### 7. Address Pair Analysis
Finds high-frequency trading between specific address pairs.

## 📊 Output Examples

### Reports
- `analysis_report_TIMESTAMP.txt` - Text summary
- `circular_trades_TIMESTAMP.csv` - Circular patterns
- `rapid_roundtrips_TIMESTAMP.csv` - Round-trip trades
- `suspicious_addresses_TIMESTAMP.txt` - Flagged addresses

### Visualizations
- `transfer_timeline.png` - Activity over time
- `volume_distribution.png` - Transfer size distribution
- `hourly_activity.png` - Activity by hour
- `top_addresses.png` - Highest volume addresses
- `network_graph.png` - Transfer network visualization

## 🔧 Configuration

Edit `.env` to customize analysis parameters:

- `TIME_RANGE`: Analysis period (7d, 30d, 90d)
- `MIN_USD_VALUE`: Minimum transfer size to include
- `MAX_TRANSFERS`: Maximum transfers to collect
- `TARGET_TOKEN`: Token address to analyze

