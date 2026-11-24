plugins {
	id("autoredistrict.application-conventions")
}

dependencies {
	implementation(project(":service"))
	implementation(libs.picocli)
}

application {
	applicationName = "autoredistrict"
	mainModule = "com.temotskipa.autoredistrict.cli"
	mainClass = "com.temotskipa.autoredistrict.cli.Main"
}
