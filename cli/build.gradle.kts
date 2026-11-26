plugins {
	id("autoredistrict.application-conventions")
}

dependencies {
	implementation(project(":core"))
	implementation(libs.picocli)
}

application {
	applicationName = "autoredistrict"
	mainModule = "com.temotskipa.autoredistrict.cli"
	mainClass = "com.temotskipa.autoredistrict.cli.Main"
}

if (System.getProperty("os.name").startsWith("Windows")) {
	// The command line app needs a console window.
	listOf("jpackageImage", "jpackageInstaller").forEach {
		tasks.named<Exec>(it) {
			args("--win-console")
		}
	}
	// A per-user install (no administrator rights) in a folder you can choose. The upgrade code
	// stays fixed, so a newer installer replaces an older one.
	tasks.named<Exec>("jpackageInstaller") {
		args(
			"--description", "Command line for autoredistrict: import RDH data and draw seat-targeted maps",
			"--win-per-user-install", "--win-dir-chooser",
			"--win-upgrade-uuid", "12a941a2-b00e-4248-b57d-a4c863b8b55e",
		)
	}
}
