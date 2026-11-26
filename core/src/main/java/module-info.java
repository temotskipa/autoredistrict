/**
 * Everything both front ends share, in three packages: {@code data} (Redistricting Data Hub import
 * from shapefiles and CSVs, the per-state Albers projection, adjacency, block votes, the tract
 * cache and the 2020 apportionment table), {@code render} (map painting over a small canvas
 * interface, Java2D here and JavaFX in the GUI, PNGs, the contact sheet, and GeoJSON, shapefile
 * and CSV export) and {@code service} (plan requests, the graph-Voronoi start, schedules, metrics
 * and PASS checks, the batch, seat range and artifacts).
 */
module com.temotskipa.autoredistrict.core {
    requires transitive com.temotskipa.autoredistrict.engine;
    // JTS geometries are part of this module's API.
    requires transitive org.locationtech.jts;
    requires transitive tools.jackson.databind;
    // The canvas interface and the PNG exports use java.awt types.
    requires transitive java.desktop;
    
    exports com.temotskipa.autoredistrict.data;
    exports com.temotskipa.autoredistrict.render;
    exports com.temotskipa.autoredistrict.service;
    // Jackson reads and writes the cache manifest records and writes the JSON plan report.
    opens com.temotskipa.autoredistrict.data to tools.jackson.databind;
    opens com.temotskipa.autoredistrict.service to tools.jackson.databind;
}
