git submodule update --init --recursive

sudo apt-get update
sudo apt-get install -y build-essential libssl-dev pkg-config

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup override set nightly
