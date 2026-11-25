/**
 * The JavaFX desktop app, including snapshot mode.
 */
module com.temotskipa.autoredistrict.gui {
    requires com.temotskipa.autoredistrict.core;
    requires javafx.controls;
    requires atlantafx.base;
    
    // JavaFX instantiates the Application subclass reflectively.
    exports com.temotskipa.autoredistrict.gui to javafx.graphics;
}
