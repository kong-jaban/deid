#!/usr/bin/env bash

userid=aaaa
useremail=aaaa@gmail.com

#
# git
#
# sudo apt install -y git
git config --global user.name $userid
git config --global user.email $useremail
git config --global pull.rebase false	# merge
git config --global init.defaultBranch main

mkdir ~/ws
cd ~/ws
git clone https://github.com/upsdata/deid.git

echo "export PYTHONPATH=~/ws/deid:\$PYTHONPATH"  >>  ~/.bashrc

