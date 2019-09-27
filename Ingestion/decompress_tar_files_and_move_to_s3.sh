#!/bin/bash

file="list_file__00.txt"

while read -r line;  do
echo "aws s3 cp s3://insighttwitterdeals/Twitter_dumps/$line Twitter_data/"
echo tar -xvf $line
echo aws s3 mv 2018/ s3://insighttwitterdeals/Twitter_dumps/bz2_folder/ --recursive
done < "$file" 

