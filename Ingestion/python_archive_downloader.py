#!/usr/bin/env python3

import os

#TODO Make sure the modify it for every different month or archive source
internet_root_url = "https://archive.org/download/archiveteam-twitter-stream-2018-06/"
for num in range(1,31):
    cmd_txt = 'wget ' + internet_root_url + "twitter-2018-06-{0:02d}.tar".format(num)
    os.system(cmd_txt)


