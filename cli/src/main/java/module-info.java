/**
 * The {@code autoredistrict} command line: import, run, all and seat-range.
 */
module com.temotskipa.autoredistrict.cli {
    requires com.temotskipa.autoredistrict.service;
    requires info.picocli;

    // picocli sets the annotated option fields reflectively.
    opens com.temotskipa.autoredistrict.cli to info.picocli;
}
