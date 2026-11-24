plugins {
	id("autoredistrict.java-conventions")
}

dependencies {
	// Exposed in the API: JTS geometries.
	api(libs.jts.core)
	implementation(libs.jackson.databind)
	implementation(libs.gt.api)
	implementation(libs.gt.main)
	implementation(libs.gt.shapefile)
	implementation(libs.gt.referencing)
	// GeoTools' units library systems-common requires the module systems.uom.quantity in its module-info,
	// but its POM marks that jar "provided", so it never arrives. The module path refuses to start without it.
	runtimeOnly(libs.systems.quantity)
}
