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
    mainClass = "autoredistrict.gui.Launcher"
}
