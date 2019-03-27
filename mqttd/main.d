int main() {
    import mqttd.runtime: vibemain;
    try {
        vibemain;
        return 0;
    } catch(Exception e) {
        import std.experimental.logger: error;
        error("Error: ", e.msg);
        return 1;
    }
}
