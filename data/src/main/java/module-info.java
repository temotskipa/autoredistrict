/**
 * Redistricting Data Hub import (GeoTools), the per-state Albers projection, adjacency, block
 * votes, the tract cache and the 2020 apportionment table.
 */
module com.temotskipa.autoredistrict.data {
    requires transitive com.temotskipa.autoredistrict.engine;
    requires transitive org.locationtech.jts;
    requires transitive tools.jackson.databind;

    requires org.geotools.api;
    requires org.geotools.main;
    requires org.geotools.referencing;
    requires org.geotools.shapefile;

    // GeoTools' jars are automatic modules, which declare no requires; these are the JDK modules
    // jdeps lists for them, so they are in the module graph whenever this module is.
    requires java.desktop;
    requires java.naming;
    requires java.prefs;
    requires java.rmi;
    requires java.scripting;
    requires java.sql;

    exports com.temotskipa.autoredistrict.data;
    // The cache manifest records are read and written by Jackson.
    opens com.temotskipa.autoredistrict.data to tools.jackson.databind;
}
