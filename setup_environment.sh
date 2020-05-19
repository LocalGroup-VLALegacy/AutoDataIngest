#!/bin/bash

sudo apt update
sudo apt upgrade
sudo apt install nano

# Download and install miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# Require interaction
./Miniconda3-latest-Linux-x86_64.sh

# Globus CLI
pip install --upgrade --user globus-cli

# These install into the .local/bin folder
echo "export PATH=$PATH:/home/ekoch/.local/bin" >> ~/.bashrc
source ~/.bashrc

# CANFAR vofs
pip install --upgrade --user vofs

# We also want some astropy packages
conda install -y astropy astroquery

# Eventually we'll need plotly, too
conda install -y plotly
