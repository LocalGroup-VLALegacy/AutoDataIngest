#!/bin/bash

sudo apt update
sudo apt upgrade
sudo apt install nano less

# Download and install miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# Require interaction
./Miniconda3-latest-Linux-x86_64.sh

# Globus CLI
pip install --upgrade --user globus-cli

# These install into the .local/bin folder
echo "export PATH=$PATH:/home/ekoch/.local/bin" >> ~/.bashrc
source ~/.bashrc

# Update globus (if needed)
globus update

# CANFAR vofs
pip install --upgrade --user vofs

# We also want some astropy packages
conda install -y astropy

# There's a bug fix we need from astroquery that is only in the
# dev version as of 10/14/20
git clone https://github.com/astropy/astroquery.git
cd astroquery
pip install -e .
cd ../

# Eventually we'll need plotly, too
conda install -y plotly

# Install firefox and xvfb
sudo apt install firefox xvfb
# Install geckodriver
# https://askubuntu.com/questions/870530/how-to-install-geckodriver-in-ubuntu
wget https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz
tar zxf geckodriver-v0.26.0-linux64.tar.gz
# executable now in home directory

# And selenium
conda install -y selenium

# Other python packages
# For accessing and updating google sheets:
pip install gspread
# Handy cell formatting in google sheets:
pip install gspread-formatting

# Read and access email contents. Used for notifications to update stage
# of track reduction, etc
pip install ezgmail
# If the latter fails, run:
# pip uninstall -y enum34
# Unsure why enum34 is getting installed for py >3.7...

# Like cron, but in python and easier to use:
pip install schedule


# Install globus here. This require some manual steps to setup the endpoint.
# But it need to be running at all times:
cd globusconnectpersonal-3.1.2
./globusconnectpersonal -start &
cd ../