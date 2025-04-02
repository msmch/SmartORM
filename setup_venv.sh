#! /bin/bash
# Create virtual env for the project if it doesn't exist already

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python -m venv .venv
fi

echo "Activating virtual environment..."
source .venv/Scripts/activate

echo "Installing dependencies..."
python -m pip install --upgrade pip
pip install -r requirements.txt

echo "Setup complete!"