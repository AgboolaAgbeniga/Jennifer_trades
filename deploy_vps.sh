#!/bin/bash
# Polymarket Arb Bot - VPS Deployment Script

echo "--- Starting VPS Setup ---"

# 1. Update and install dependencies
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv git sqlite3

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install requirements
pip install --upgrade pip
pip install -r requirements.txt

# 4. Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env template... Please fill this in later."
    cp .env.example .env
fi

# 5. Setup Systemd service for 24/7 uptime
cat <<EOF | sudo tee /etc/systemd/system/arb-bot.service
[Unit]
Description=Polymarket Arbitrage Bot
After=network.target

[Service]
User=$USER
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/venv/bin/python polymarket_arb_bot.py --balance 1000
Restart=always
RestartSec=10
StandardOutput=append:$(pwd)/arb_bot_v2.log
StandardError=append:$(pwd)/arb_bot_v2.log

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
echo "--- Setup Complete ---"
echo "Next steps:"
echo "1. Edit your .env file with your real keys."
echo "2. Start the bot: sudo systemctl start arb-bot"
echo "3. View logs: tail -f arb_bot_v2.log"
