plugins {
	id("autoredistrict.application-conventions")
}

dependencies {
	implementation(project(":service"))
	implementation(variantOf(libs.javafx.base) { classifier("win") })
	implementation(variantOf(libs.javafx.graphics) { classifier("win") })
	implementation(variantOf(libs.javafx.controls) { classifier("win") })
	implementation(libs.atlantafx)
}

application {
	applicationName = "autoredistrict-gui"
	mainModule = "com.temotskipa.autoredistrict.gui"
	mainClass = "com.temotskipa.autoredistrict.gui.Launcher"
	applicationDefaultJvmArgs = JvmFlags.gui
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
				"if [ -f \"\$APP_HOME/lib/gui.aot\" ]; then\n" +
						"    DEFAULT_JVM_OPTS=\"\$DEFAULT_JVM_OPTS \\\"-XX:AOTCache=\$APP_HOME/lib/gui.aot\\\"\"\n" +
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
