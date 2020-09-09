#!/bin/bash

# Simple utility to download files from public google drives
# USAGE: ./wget-drive.sh [drive id] [save to this filename]

if [ ! $# -eq 2 ]; then
    echo "USAGE: ./wget-drive.sh [drive id] [save to this filename]"
else
    wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate "https://docs.google.com/uc?export=download&id=$1" -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=$1" -O $2 && rm -rf /tmp/cookies.txt
fi