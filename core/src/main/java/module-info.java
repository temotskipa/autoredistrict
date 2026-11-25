/**
 * Everything both front ends share, in three packages: {@code data} (Redistricting Data Hub import
 * with GeoTools, the per-state Albers projection, adjacency, block votes, the tract cache and the
 * 2020 apportionment table), {@code render} (map painting over a small canvas interface, Java2D
 * here and JavaFX in the GUI, PNGs, the contact sheet, and GeoJSON, shapefile and CSV export) and
 * {@code service} (plan requests, the graph-Voronoi start, schedules, metrics and PASS checks, the
 * batch, seat range and artifacts).
 */
// JTS geometries are part of this module's API, and JTS only ships as an automatic module.
@SuppressWarnings("requires-transitive-automatic")
module com.temotskipa.autoredistrict.core {
    requires transitive com.temotskipa.autoredistrict.engine;
    requires transitive org.locationtech.jts;
    requires transitive tools.jackson.databind;
    // The canvas interface and the PNG exports use java.awt types.
    requires transitive java.desktop;
    
    requires org.geotools.api;
    requires org.geotools.main;
    requires org.geotools.referencing;
    requires org.geotools.shapefile;
    
    // GeoTools' jars are automatic modules, which declare no requires; these are the JDK modules
    // jdeps lists for them, so they are in the module graph whenever this module is.
    requires java.naming;
    requires java.prefs;
    requires java.rmi;
    requires java.scripting;
    requires java.sql;
    
    exports com.temotskipa.autoredistrict.data;
    exports com.temotskipa.autoredistrict.render;
    exports com.temotskipa.autoredistrict.service;
    // Jackson reads and writes the cache manifest records and writes the JSON plan report.
    opens com.temotskipa.autoredistrict.data to tools.jackson.databind;
    opens com.temotskipa.autoredistrict.service to tools.jackson.databind;
}
