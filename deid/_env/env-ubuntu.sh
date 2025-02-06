#!/usr/bin/env bash

#
# brave browser
#
sudo curl -fsSLo /usr/share/keyrings/brave-browser-archive-keyring.gpg \
https://brave-browser-apt-release.s3.brave.com/brave-browser-archive-keyring.gpg

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/brave-browser-archive-keyring.gpg] \
https://brave-browser-apt-release.s3.brave.com/ stable main" \
| sudo tee /etc/apt/sources.list.d/brave-browser-release.list
sudo apt update
sudo apt install -y brave-browser

#
# vs code
#
cd ~/Downloads
wget -qO- https://packages.microsoft.com/keys/microsoft.asc \
| gpg --dearmor > packages.microsoft.gpg
sudo install -o root -g root -m 644 packages.microsoft.gpg /etc/apt/trusted.gpg.d/
echo "deb [arch=amd64 signed-by=/etc/apt/trusted.gpg.d/packages.microsoft.gpg] \
https://packages.microsoft.com/repos/vscode stable main" \
| sudo tee /etc/apt/sources.list.d/vscode.list
sudo apt update
sudo apt install -y code

#
# D2Coding font
#
d2name=D2Coding-Ver1.3.2-20180524.zip
wget https://github.com/naver/d2codingfont/releases/download/VER1.3.2/$d2name
unzip $d2name
sudo mkdir /usr/share/fonts/truetype/D2Coding
sudo cp ./D2Coding/*.ttf /usr/share/fonts/truetype/D2Coding
fc-cache -v

sudo apt autoremove -y

