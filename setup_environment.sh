#!/bin/bash

sudo apt update
sudo apt upgrade -y
sudo apt install nano less -y

# Setup folder structure
# We'll keep the job files stashed here and on gdrive since they're only txt files.
mkdir reduction_job_scripts

# Download and install miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# Require interaction
./Miniconda3-latest-Linux-x86_64.sh

# Globus CLI
pip install --upgrade --user globus-cli

# These install into the .local/bin folder
echo "export PATH=$PATH:/home/datamanager/.local/bin" >> ~/.bashrc
source ~/.bashrc

# Update globus (if needed)
globus update

# CANFAR vofs
pip install --upgrade --user vofs

# We also want some astropy packages
conda install -y astropy ipython numpy scipy matplotlib

# There's a bug fix we need from astroquery that is only in the
# dev version as of 10/14/20
# git clone https://github.com/astropy/astroquery.git
# cd astroquery
# pip install -e .
# cd ../
# Should be able to use release v0.4 as of 04/2021
pip install astroquery

# Eventually we'll need plotly, too
conda install -y plotly

# Install firefox and xvfb
sudo apt install firefox xvfb
# Install geckodriver
# https://askubuntu.com/questions/870530/how-to-install-geckodriver-in-ubuntu
wget https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz
tar zxf geckodriver-v0.26.0-linux64.tar.gz
# executable now in home directory, move to somewhere in PATH
mv geckodriver .local/bin/
rm -rf geckodriver-v0.26.0*.tar.gz


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
# Not using right now.
# pip install schedule

# fabric handles ssh connections in python
pip install fabric

# timeout context handler for async functions
pip install async_timeout

# Install globus here. This require some manual steps to setup the endpoint.
# But it need to be running at all times:
sudo apt-get install tk tcllib
$ wget https://downloads.globus.org/globus-connect-personal/linux/stable/globusconnectpersonal-latest.tgz
tar -xf globusconnectpersonal-latest.tgz
cd globusconnectpersonal-3.1.4
# Do the first time setup
./globusconnectpersonal -setup
# YOU NEED PY37 for this to work currently! Breaks with py38...
./globusconnectpersonal -start &
cd ../

# You need the service_account.json file setup for gspread:
# Will go into .local/gspread/service_account.json
# Ask Eric about the account credentials attached to the shared gdrive

# Next, setup ezgmail:
# Will need oath2 credentials created.
# See https://ezgmail.readthedocs.io/en/latest/
# If token.json and credentials.json are in the home directory,
# this should work:
python -c "import ezgmail; ezgmail.init()"

