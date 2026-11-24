// Convention plugins (Java 28 toolchain, preview flags, JPMS launch) live in the build-logic included build.
pluginManagement {
	includeBuild("build-logic")
}

rootProject.name = "autoredistrict"

// engine <- data <- service -> render; cli and gui are the two front ends on top of service.
include("engine", "data", "render", "service", "cli", "gui")
