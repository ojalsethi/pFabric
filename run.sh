#!/bin/bash

#clean up mininet
sudo mn -c

hosts=28
runtime=240
savedir="results/"
declare -a traffic=("data" "web")
declare -a cong=("tcp" "mintcp")

sudo rm -rf $savedir

for t in "${traffic[@]}"
do
    for c in "${cong[@]}"
    do
        sudo python2 pfabric.py -o $savedir -c $c -t $t -n $hosts --time $runtime
    done
        sudo pkill python2
        sudo mn -c > /dev/null
        sleep 30s
done

sudo python2 plots.py -o $savedir
