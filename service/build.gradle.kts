plugins {
	id("autoredistrict.java-conventions")
}

dependencies {
	// Front ends see the data and render types through the service API (and the engine through data).
	api(project(":data"))
	api(project(":render"))
}
