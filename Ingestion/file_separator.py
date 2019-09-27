#!/bin/python3
import os
import csv
filename = 'list_files.txt'
output_file_name_prefix = 'list_file_'
NUM_LINES = 133
num_lines_per_file = int(133/5)+1
# num_lines_to_write = []
lines_per_file = [[] for i in range(5)]
# for i in range(5):
#     if i<4:
#         num_lines_to_write[i] = num_lines_per_file
#     else:
#         num_lines_to_write[i] = 133 - 4*num_lines_per_file


with open(filename,'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=' ')
    line_count = 0
    for row in csv_reader:
        print(row)
        lines_per_file[int(line_count/num_lines_per_file)].append(row[0])
        line_count +=1
    print("Procesed {} lines".format(line_count))


for i in range(5):
    output_file = output_file_name_prefix + "_{0:02d}.txt".format(i)
    with open(output_file, 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        for line in lines_per_file[i]:
            csv_writer.writerow([line])

