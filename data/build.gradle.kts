plugins {
	id("autoredistrict.java-conventions")
}

dependencies {
	api(project(":engine"))
	api(libs.jts.core)
	api(libs.jackson.databind)
	implementation(libs.gt.shapefile)
	implementation(libs.gt.referencing)
}
