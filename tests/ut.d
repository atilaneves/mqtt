#!/usr/bin/rdmd -Isource


import unit_threaded.runner;


int main(string[] args) {
    return runTests!("encode", "broker", "server")(args);
}
