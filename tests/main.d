import unit_threaded.runner: runTestsMain;


mixin runTestsMain!(
    "tests.broker",
    "tests.stream",
    "tests.message",
    "tests.server"
);
