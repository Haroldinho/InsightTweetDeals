#!/bin/bash

cd /home/ubuntu/
 
wget "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
chmod u+x Miniconda3-latest-Linux-x86_64.sh
/bin/bash Miniconda3-latest-Linux-x86_64.sh -b
echo  'PATH="/home/ubuntu/miniconda3/bin:$PATH"' >> ~/.bashrc
source home/ubuntu/.bashrc
source /home/ubuntu/miniconda3/bin/activate

cd ~
conda create --name spacypython --file spec_file.txt
echo "Inserted by script"
echo "source ~/miniconda3/bin/activate" >> "/home/ubuntu/.bashrc"
echo "source activate spacypython" >> "/home/ubuntu/.bashrc"
source /home/ubuntu/.bashrc
source ~/miniconda3/bin/activate
source activate spacypython
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_md
python -m spacy download en_core_web_lg
