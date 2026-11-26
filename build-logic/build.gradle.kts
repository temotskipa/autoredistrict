plugins {
	`kotlin-dsl`
}

dependencies {
	// Gives JTS, which ships without a module-info, a real one, so jlink can link it.
	implementation("org.gradlex:extra-java-module-info:1.14.2")
}
