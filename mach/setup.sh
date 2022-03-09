#! /usr/bin/env bash

rustup override set nightly
tar -xzf data.tar.gz
cd data
wget https://www.dropbox.com/s/ya7mwmvq17v6hqh/data.tar.gz data/data.tar.gz
tar -xzf data.tar.gz
cd ../
