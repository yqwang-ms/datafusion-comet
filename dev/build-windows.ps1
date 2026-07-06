<#
.SYNOPSIS
    Build Apache DataFusion Comet on Windows.

.DESCRIPTION
    There is no `make` on a stock Windows install, and a few Comet build steps need
    Windows-specific handling (the native crate must be built without the Unix-only
    HDFS backend, and the Maven wrapper must be invoked so the `-P<profile>` argument
    is not mangled by PowerShell). This script wraps the two build steps:

        1. cargo build [--release] --no-default-features   (produces native\target\<profile>\comet.dll)
        2. mvnw.cmd -P<spark-profile> package               (bundles comet.dll into the Spark jar)

    Native HDFS is intentionally disabled on Windows: it depends on the libhdfs C
    bindings (hdfs-sys / hdrs) which do not build with MSVC. Object-store backends
    (S3, Azure, GCS) are unaffected and remain enabled.

.PARAMETER Release
    Build the optimized release profile. Default is a debug build.

.PARAMETER SparkProfile
    The Spark Maven profile to activate (default: spark-4.1). Spark 4.x profiles
    compile with Java 17; Spark 3.x profiles compile with Java 11.

.PARAMETER SkipTests
    Pass -DskipTests to Maven (default: $true). Set to $false to run the JVM tests.

.PARAMETER Features
    Optional comma-separated list of extra Cargo features to enable (on top of the
    default, HDFS-free, Windows feature set).

.EXAMPLE
    pwsh -File dev/build-windows.ps1

.EXAMPLE
    pwsh -File dev/build-windows.ps1 -Release -SparkProfile spark-3.5
#>
[CmdletBinding()]
param(
    [switch]$Release,
    [string]$SparkProfile = "spark-4.1",
    [bool]$SkipTests = $true,
    [string]$Features = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Assert-Command($name, $hint) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        throw "'$name' was not found on PATH. $hint"
    }
}

Write-Host "==> Checking prerequisites" -ForegroundColor Cyan
Assert-Command "cargo" "Install Rust from https://rustup.rs (the default x86_64-pc-windows-msvc toolchain)."
Assert-Command "protoc" "Install Protocol Buffers >= 3.x and put protoc.exe on PATH (or set the PROTOC env var)."
if (-not $env:JAVA_HOME) {
    throw "JAVA_HOME is not set. Point it at a JDK 17 (Spark 4.x) or JDK 11 (Spark 3.x) install."
}
Write-Host "    cargo:     $(cargo --version)"
Write-Host "    protoc:    $(protoc --version)"
Write-Host "    JAVA_HOME: $env:JAVA_HOME"

# --- 1. Native build (comet.dll) ---------------------------------------------
$cargoArgs = @("build", "--no-default-features")
if ($Release) { $cargoArgs += "--release" }
if ($Features) { $cargoArgs += @("--features", $Features) }

Write-Host "==> Building native library: cargo $($cargoArgs -join ' ')" -ForegroundColor Cyan
Push-Location native
try {
    & cargo @cargoArgs
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed with exit code $LASTEXITCODE" }
}
finally {
    Pop-Location
}

$profileDir = if ($Release) { "release" } else { "debug" }
$dll = Join-Path $repoRoot "native\target\$profileDir\comet.dll"
if (-not (Test-Path $dll)) { throw "Expected native library not found: $dll" }
Write-Host "    Built $dll" -ForegroundColor Green

# --- 2. JVM build / packaging ------------------------------------------------
# Invoke the Maven wrapper through cmd.exe so PowerShell does not split the
# '-P<profile>' argument on the '.' in the profile name (e.g. spark-4.1).
$mvnArgs = "-P$SparkProfile"
if ($Release) { $mvnArgs += " -Prelease" }
if ($SkipTests) { $mvnArgs += " -DskipTests" }
$mvnArgs += " package"

Write-Host "==> Building JVM modules: mvnw.cmd $mvnArgs" -ForegroundColor Cyan
& cmd /c "mvnw.cmd $mvnArgs"
if ($LASTEXITCODE -ne 0) { throw "Maven build failed with exit code $LASTEXITCODE" }

$jar = Get-ChildItem "spark\target\comet-spark-*.jar" -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -notmatch 'sources|javadoc|tests|^original-' } |
    Select-Object -First 1
Write-Host ""
Write-Host "==> Build complete." -ForegroundColor Green
if ($jar) { Write-Host "    Comet Spark jar: $($jar.FullName)" -ForegroundColor Green }
