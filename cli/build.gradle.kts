plugins {
    id("autoredistrict.application-conventions")
}

dependencies {
    implementation(project(":service"))
    implementation(libs.picocli)
}

application {
    applicationName = "autoredistrict"
    mainClass = "autoredistrict.cli.Main"
}
