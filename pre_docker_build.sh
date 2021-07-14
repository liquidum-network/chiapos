#!/usr/bin/env bash

if [ "$(uname)" == "Darwin" ]; then
  #running on Mac OS
  echo "Install Docker Desktop on Mac"
elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ] || [ "$(expr substr $(uname -s) 1 10)" == "MINGW64_NT" ]; then
  #running on Windows
  echo "Install Docker Desktop on Windows"
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
  if ! docker -v &>/dev/null; then
    echo "Installing docker"
    sudo apt-get update
    sudo apt-get install \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg \
      lsb-release
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo apt-get update
    sudo apt-get install docker-ce docker-ce-cli containerd.io
  fi
  if ! docker-compose -v &>/dev/null; then
    echo "Installing docker-compose"
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
  fi
fi
mkdir -p ./build/docker_files
aws s3 cp s3://chiamine-us-west-2/plotter-builds/latest/bootstrap.tar.gz ./build/docker_files/
