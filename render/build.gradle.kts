plugins {
	id("autoredistrict.java-conventions")
}

dependencies {
	api(project(":data"))
	implementation(libs.gt.shapefile)
	implementation(libs.gt.referencing)
}
