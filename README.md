# Polymarket Latency Arbitrage Bot ⚡

A high-performance trading bot designed for Polymarket arbitrage using real-time price signals from multiple centralized exchanges (CEX).

> [!IMPORTANT]
> This bot includes specialized **Nigeria Multi-Feed** logic to bypass regional connectivity issues by using a consensus of OKX, Bybit, and Kraken feeds.

## 🚀 Key Features
- **Latency Arbitrage**: Captures price discrepancies between Polymarket and CEX feeds (BTC/ETH).
- **Consensus Pricing**: Uses median price from OKX, Bybit, and Kraken for maximum signal robustness.
- **Risk Management**: Integrated Kelly Criterion position sizing, daily stop-loss, and total drawdown kill-switch.
- **Paper Trading Mode**: Full simulation engine to test strategies without real capital.
- **Rich Dashboard**: Beautiful real-time terminal UI for monitoring feeds and performance.
- **VPS Ready**: Includes `deploy_vps.sh` for easy deployment on Oracle Cloud or AWS.

## 🛠 Setup

### 1. Requirements
- Python 3.10+
- Polymarket API Credentials (API Key, Secret, Passphrase)
- Polygon Private Key (MetaMask/EOA or Gnosis Safe)

### 2. Installation
```bash
git clone <your-repo-url>
cd Jennifer_trades
pip install -r requirements.txt
```

### 3. Configuration
Copy `.env.example` to `.env` and fill in your details:
```bash
POLY_PRIVATE_KEY=your_key
POLY_FUNDER_ADDRESS=your_address
# ... other API keys
```

## 🧪 Running the Bot

### Paper Trading (Safe Mode)
```bash
python polymarket_arb_bot.py --balance 1000
```

### Live Trading
```bash
python polymarket_arb_bot.py --live
```

## ☁️ VPS Deployment (Oracle Cloud)
1. Create an Ubuntu instance (ARM Ampere recommended).
2. Open **Egress** in your VCN Security List (0.0.0.0/0).
3. SSH into your instance and run:
   ```bash
   chmod +x deploy_vps.sh
   ./deploy_vps.sh
   ```
4. Start the service: `sudo systemctl start arb-bot`

## 📊 Strategy Overview
The bot monitors the **Price Gap** between Polymarket and the CEX consensus. When a significant move occurs on CEX that hasn't reflected on Polymarket yet, the bot calculates the **Edge** and uses the **Kelly Criterion** to determine the optimal bet size.

## ⚖️ Disclaimer
Trading involves risk. This bot is provided for educational purposes. Always start with Paper Mode to validate your strategy and risk parameters.
