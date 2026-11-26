plugins {
	id("autoredistrict.application-conventions")
}

// JavaFX ships its native libraries per platform; build for the host (win, mac, mac-aarch64, linux,
// linux-aarch64). Set -Pjavafx.platform=<classifier> to build for another one.
val javafxPlatform: String = providers.gradleProperty("javafx.platform").orNull ?: run {
	val os = System.getProperty("os.name").lowercase()
	val arm = System.getProperty("os.arch").lowercase() in setOf("aarch64", "arm64")
	when {
		os.startsWith("windows") -> "win"
		os.startsWith("mac") -> if (arm) "mac-aarch64" else "mac"
		else -> if (arm) "linux-aarch64" else "linux"
	}
}

dependencies {
	implementation(project(":core"))
	implementation(variantOf(libs.javafx.base) { classifier(javafxPlatform) })
	implementation(variantOf(libs.javafx.graphics) { classifier(javafxPlatform) })
	implementation(variantOf(libs.javafx.controls) { classifier(javafxPlatform) })
	implementation(libs.atlantafx)
}

application {
	applicationName = "autoredistrict-gui"
	mainModule = "com.temotskipa.autoredistrict.gui"
	mainClass = "com.temotskipa.autoredistrict.gui.Launcher"
	applicationDefaultJvmArgs = JvmFlags.gui
}

// The Windows installer: a per-user install (no administrator rights) in a folder you can choose,
// with a Start menu entry and a desktop shortcut. The upgrade code stays fixed, so a newer
// installer replaces an older one.
if (System.getProperty("os.name").startsWith("Windows")) {
	tasks.named<Exec>("jpackageInstaller") {
		args(
			"--description", "Draws congressional maps that deliver a requested number of Democratic seats",
			"--win-per-user-install", "--win-dir-chooser",
			"--win-menu", "--win-menu-group", "autoredistrict", "--win-shortcut",
			"--win-upgrade-uuid", "f027b5fb-4ff9-4b66-9ec0-e33b6bcabfa1",
		)
	}
}

// AOT cache (JEP 483/514/515): the start script uses lib/gui.aot when it exists. A cache that
// doesn't exist makes the JVM print errors, so the option is added only if the file is there.
tasks.named<CreateStartScripts>("startScripts") {
	doLast {
		windowsScript.writeText(
			windowsScript.readText().replace(
				"@rem Find java.exe",
				"if exist \"%APP_HOME%\\lib\\gui.aot\" set DEFAULT_JVM_OPTS=%DEFAULT_JVM_OPTS% " +
						"\"-XX:AOTCache=%APP_HOME%\\lib\\gui.aot\"\r\n\r\n@rem Find java.exe",
			)
		)
		unixScript.writeText(
			unixScript.readText().replaceFirst(
				"# Determine the Java command to use",
				$$"if [ -f \"$APP_HOME/lib/gui.aot\" ]; then\n" +
						$$"    DEFAULT_JVM_OPTS=\"$DEFAULT_JVM_OPTS \\\"-XX:AOTCache=$APP_HOME/lib/gui.aot\\\"\"\n" +
						"fi\n\n# Determine the Java command to use",
			)
		)
	}
}

val installDir = layout.buildDirectory.dir("install/autoredistrict-gui")

// Training run for the AOT cache: the ME snapshot, through the installed start script so the
// class path matches exactly. Then the start script picks up lib/gui.aot automatically.
tasks.register<Exec>("aotCache") {
	group = "distribution"
	description = "Builds lib/gui.aot in the installed GUI with a training run (ME snapshot)."
	dependsOn(tasks.named("installDist"))
	workingDir = rootDir
	val dir = installDir.get().asFile
	val aot = File(dir, "lib/gui.aot")
	val training = layout.buildDirectory.file("aot/training.png").get().asFile
	doFirst {
		aot.delete()
		training.parentFile.mkdirs()
	}
	environment("JAVA_OPTS", "-XX:AOTCacheOutput=${aot.absolutePath}")
	val windows = System.getProperty("os.name").startsWith("Windows")
	val script = File(dir, if (windows) "bin/autoredistrict-gui.bat" else "bin/autoredistrict-gui")
	val args = listOf("--snapshot", "ME", "--dem-seats", "1", "--seeds", "4", "--out", training.absolutePath)
	if (windows) {
		commandLine(listOf("cmd", "/c", script.absolutePath) + args)
	} else {
		commandLine(listOf(script.absolutePath) + args)
	}
}
