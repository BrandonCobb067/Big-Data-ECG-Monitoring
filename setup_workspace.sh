#!/bin/bash
set -euo pipefail

echo "============================================================"
echo "          MODULAR ECG PIPELINE WORKSPACE SETUP              "
echo "============================================================"

# Check for wget
if ! command -v wget &> /dev/null; then
    echo "Error: 'wget' is not installed. Please install it and try again."
    exit 1
fi

BASE_DIR="$(pwd)"

# Create data directory structure
echo "[1/4] Scaffolding directory structure..."
mkdir -p "$BASE_DIR"/data/chapman/{raw,cleaned}
mkdir -p "$BASE_DIR"/data/ieee_peco/{raw,cleaned}
mkdir -p "$BASE_DIR"/data/ieee_transfer_learning/{raw,cleaned}
mkdir -p "$BASE_DIR"/data/ptbxl/{raw,cleaned}
mkdir -p "$BASE_DIR"/data/mitbih/{raw,cleaned}

# Helper function for downloading
download_dataset() {
    local step=$1
    local name=$2
    local url=$3
    local dest_path=$4

    echo "${step} Downloading ${name}..."
    if [ -f "$dest_path" ]; then
        echo "      -> File already exists. Skipping."
    else
        wget -c -O "$dest_path" "$url"
    fi
}

# Downloadable datasets
download_dataset "[2/4]" "MIT-BIH dataset" \
    "https://physionet.org/static/published-projects/mitdb/mit-bih-arrhythmia-database-1.0.0.zip" \
    "$BASE_DIR/data/mitbih/raw/mit-bih-dataset.zip"

download_dataset "[3/4]" "PTB-XL dataset" \
    "https://physionet.org/static/published-projects/ptb-xl/ptb-xl-a-large-publicly-available-electrocardiography-dataset-1.0.3.zip" \
    "$BASE_DIR/data/ptbxl/raw/ptb-xl-dataset.zip"

download_dataset "[4/4]" "PECO dataset" \
    "https://github.com/ben012346/PCG-ECG-Co-Ordination-PECO-Dataset/archive/refs/heads/main.zip" \
    "$BASE_DIR/data/ieee_peco/raw/peco-dataset.zip"

echo "============================================================"
echo "                   AUTOMATION COMPLETE                      "
echo "============================================================"
echo ""
echo "ACTION REQUIRED: Manual Downloads for Remaining Datasets"
echo "------------------------------------------------------------"

# Validation for Chapman
if [ -f "$BASE_DIR/data/chapman/raw/ECGData.zip" ]; then
    echo "[✓] Chapman Dataset found."
else
    echo "[ ] 1. Chapman Dataset MISSING:"
    echo "       - Go to: https://figshare.com/collections/_/4560497"
    echo "       - Download: ECGData.zip, AttributesDictionary.xlsx, ConditionNames.xlsx, Diagnostics.xlsx, and RhythmNames.xlsx"
    echo "       - Move them to: data/chapman/raw/"
fi
echo ""

# Validation for IEEE Transfer Learning
if [ -f "$BASE_DIR/data/ieee_transfer_learning/raw/DataFall.zip" ]; then
    echo "[✓] IEEE Transfer Learning Dataset found."
else
    echo "[ ] 2. IEEE Transfer Learning Dataset MISSING:"
    echo "       - Go to: https://ieee-dataport.org/open-access/ecg-data-deep-transfer-learning"
    echo "       - Download: DataFall.zip"
    echo "       - Move it to: data/ieee_transfer_learning/raw/"
fi