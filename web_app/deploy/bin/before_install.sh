#!/bin/bash
# Adapted from https://github.com/NanchoKibo/aws-nodejs
# Install node.js if needed
cd $PWD
if which node /dev/null
then
echo "Node is installed"
else
echo "Installing Node"
curl -sL https://rpm.nodesource.com/setup_15.x | sudo bash -
sudo yum install -y nodejs
fi

cd $PWD
# Install forever module if needed
# https://www.npmjs.com/package/forever
if [ `npm list -g | grep -c forever` -eq 0 ]; then
sudo cd $PWD; npm install forever -g
fi
