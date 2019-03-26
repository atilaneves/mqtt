module mqttd.log;


void error(A...)(auto ref A args) @trusted {
    import std.functional: forward;
    import std.stdio: stderr;
    try
        stderr.writeln(forward!args);
    catch(Exception _)
        assert(0);
}
