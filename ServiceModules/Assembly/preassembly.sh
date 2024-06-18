#!/bin/bash

# Minimal launcher for the Shoestring Assembler

# Get location of this script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Download Shoestring Assembler
echo "Downloading Shoestring Assembler..."
git clone https://github.com/DigitalShoestringSolutions/ShoestringAssembler -b v0.1.0 $SCRIPT_DIR/ShoestringAssembler

# Run Shotestring Assembler
echo "Running Shoestring Assembler..."
$SCRIPT_DIR/ShoestringAssembler/assemble.sh
