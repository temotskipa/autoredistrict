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
}
