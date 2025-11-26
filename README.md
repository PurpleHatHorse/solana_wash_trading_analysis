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
- **Reporting**: Comprehensive text reports and CSV exports

## 📋 Prerequisites

- Python 3.8+
- Arkham Intelligence API key ([Get one here](https://platform.arkhamintelligence.com/))
- pip

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/PurpleHatHorse/arkham-api-token-risk-score-analysis.git
cd arkham-api-token-risk-score-analysis
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
TOKENS=EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm
CHAIN=solana
TIME_WINDOW=7d
```

### 4. Run Analysis
```bash
# Execute the main run.py
python run.py
```

## 📁 Project Structure
```
arkham-api-token-risk-score-analysis/
├── README.md
├── requirements.txt
├── run.py
└── srs
    ├── bot_detector.py
    ├── config.py
    ├── data_fetcher.py
    ├── full_risk_score_analysis.py
    ├── holder_analyzer.py
    └── wash_trading_detector.py
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

