/**
 * Plan requests, the graph-Voronoi start, schedules, metrics and PASS checks, the batch, seat
 * range and artifacts. Both front ends call this module.
 */
module com.temotskipa.autoredistrict.service {
    requires transitive com.temotskipa.autoredistrict.data;
    requires transitive com.temotskipa.autoredistrict.render;

    exports com.temotskipa.autoredistrict.service;
    // The JSON plan report is written by Jackson.
    opens com.temotskipa.autoredistrict.service to tools.jackson.databind;
}
