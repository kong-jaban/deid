#!/usr/bin/env bash

sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.11
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.11 20

sudo apt install -y curl
sudo apt install -y git

sudo apt autoremove -y
