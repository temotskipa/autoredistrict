/**
 * Map painting over a small canvas interface (Java2D here, JavaFX in the GUI), PNGs, the contact
 * sheet, and GeoJSON, shapefile and CSV export.
 */
module com.temotskipa.autoredistrict.render {
    requires transitive java.desktop;
    requires transitive org.locationtech.jts;

    requires tools.jackson.databind;
    requires org.geotools.api;
    requires org.geotools.main;
    requires org.geotools.referencing;
    requires org.geotools.shapefile;

    // JDK modules the automatic GeoTools modules use (see com.temotskipa.autoredistrict.data).
    requires java.naming;
    requires java.prefs;
    requires java.rmi;
    requires java.scripting;
    requires java.sql;

    exports com.temotskipa.autoredistrict.render;
}
