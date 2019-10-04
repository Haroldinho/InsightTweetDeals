#!/bin/bash

cd /home/ubuntu/
 
wget "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
chmod u+x Miniconda3-latest-Linux-x86_64.sh
/bin/bash Miniconda3-latest-Linux-x86_64.sh -b
source ~/miniconda3/bin/activate

cd ~
echo  'PATH="/home/ubuntu/miniconda3/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
conda create --name spacypython --file spec_file.txt
echo "Inserted by script"
echo "source ~/miniconda3/bin/activate" >> "/home/ubuntu/.bashrc"
echo "source activate spacypython" >> "/home/ubuntu/.bashrc"
source ~/.bashrc

cd ~
git clone "https://github.com/plamere/spotipy.git" "/home/ubuntu/spotipy"
cd "/home/ubuntu/spotipy"
python setup.py install
