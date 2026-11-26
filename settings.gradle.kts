// Convention plugins (Java 28 toolchain, preview flags, JPMS launch) live in the build-logic included build.
pluginManagement {
	includeBuild("build-logic")
}

rootProject.name = "autoredistrict"

// engine <- core; cli and gui are the two front ends on top of core.
include("engine", "core", "cli", "gui")
