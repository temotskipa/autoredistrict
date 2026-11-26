plugins {
	id("autoredistrict.java-conventions")
}

dependencies {
	// The data, render and service packages, shared by the CLI and the GUI.
	// Exposed in the API: the engine's Graph, JTS geometries and the shared JSON mapper.
	api(project(":engine"))
	api(libs.jts.core)
	api(libs.jackson.databind)
}
