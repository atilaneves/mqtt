//Automatically generated by dtest, do not edit by hand
import unit_threaded.runner;
import std.stdio;

int main(string[] args) {
    writeln("\nAutomatically generated file ut.d");
    writeln(`Running unit tests from dirs ["tests"]`);
    return args.runTests!(
        "tests.broker",
        "tests.encode",
        "tests.server",
        "tests.stream",
        );
}
