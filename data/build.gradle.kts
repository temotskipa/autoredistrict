plugins {
	id("autoredistrict.java-conventions")
}

dependencies {
	// Exposed in the API: the engine's Graph, JTS geometries and the shared JSON mapper.
	api(project(":engine"))
	api(libs.jts.core)
	api(libs.jackson.databind)
	implementation(libs.gt.api)
	implementation(libs.gt.main)
	implementation(libs.gt.shapefile)
	implementation(libs.gt.referencing)
}
