// Convention plugins for the autoredistrict build, included from the root settings.
dependencyResolutionManagement {
	@Suppress("UnstableApiUsage") // incubating, and the documented way to declare repositories here
	repositories {
		gradlePluginPortal()
	}
}

rootProject.name = "build-logic"
