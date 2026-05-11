# SPDX-FileCopyrightText: 2025 The superseedr Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

[CmdletBinding()]
param(
    [ValidateSet("normal", "private")]
    [string]$Flavor = "normal",

    [ValidateSet("auto", "inno", "csc", "iexpress")]
    [string]$Backend = "auto",

    [string]$Version = "",

    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$InstallerScript = Join-Path $RepoRoot "packaging\windows\superseedr-user.iss"
$OutputDir = Join-Path $RepoRoot "target\installer\windows"
$BinaryPath = Join-Path $RepoRoot "target\release\superseedr.exe"
$IconPath = Join-Path $RepoRoot "assets\app_icon.ico"

function Get-ManifestVersion {
    $cargoToml = Get-Content (Join-Path $RepoRoot "Cargo.toml")
    $versionLine = $cargoToml | Where-Object { $_ -match '^version\s*=\s*"([^"]+)"' } | Select-Object -First 1
    if ($versionLine -match '^version\s*=\s*"([^"]+)"') {
        return $Matches[1]
    }
    return "dev"
}

function Get-InnoOutputVersion {
    param(
        [Parameter(Mandatory = $true)][string]$RawVersion
    )

    $safe = $RawVersion -replace '[^A-Za-z0-9_-]', '-'
    $safe = $safe.Trim('-')
    if (-not $safe) {
        return "dev"
    }
    return $safe
}

function Find-InnoCompiler {
    $isccCandidates = @()
    $command = Get-Command "ISCC.exe" -ErrorAction SilentlyContinue
    if ($command) {
        $isccCandidates += $command.Source
    }
    $command = Get-Command "iscc" -ErrorAction SilentlyContinue
    if ($command) {
        $isccCandidates += $command.Source
    }
    if (${env:ProgramFiles(x86)}) {
        $isccCandidates += (Join-Path ${env:ProgramFiles(x86)} "Inno Setup 6\ISCC.exe")
    }
    if ($env:ProgramFiles) {
        $isccCandidates += (Join-Path $env:ProgramFiles "Inno Setup 6\ISCC.exe")
    }

    return $isccCandidates | Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1
}

function Build-WithInno {
    param(
        [Parameter(Mandatory = $true)][string]$Iscc
    )

    $outputVersion = Get-InnoOutputVersion -RawVersion $Version
    $innoArgs = @("/DAppVersion=$Version", "/DAppOutputVersion=$outputVersion")
    if ($Flavor -eq "private") {
        $innoArgs += "/DPrivateBuild"
    }

    Write-Host "Running: $Iscc $($innoArgs -join ' ') $InstallerScript"
    & $Iscc @innoArgs $InstallerScript
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Write-LocalInstallScript {
    param(
        [Parameter(Mandatory = $true)][string]$Path
    )

    @'
$ErrorActionPreference = "Stop"

$InstallDir = Join-Path $env:LOCALAPPDATA "Programs\superseedr"
$ExePath = Join-Path $InstallDir "superseedr.exe"
$IconPath = Join-Path $InstallDir "app_icon.ico"

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item -Force -Path (Join-Path $PSScriptRoot "superseedr.exe") -Destination $ExePath
Copy-Item -Force -Path (Join-Path $PSScriptRoot "app_icon.ico") -Destination $IconPath

function Set-RegistryDefault {
    param(
        [Parameter(Mandatory = $true)][string]$SubKey,
        [Parameter(Mandatory = $true)][string]$Value
    )
    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey($SubKey)
    try {
        $key.SetValue("", $Value, [Microsoft.Win32.RegistryValueKind]::String)
    } finally {
        $key.Close()
    }
}

function Set-RegistryValue {
    param(
        [Parameter(Mandatory = $true)][string]$SubKey,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value
    )
    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey($SubKey)
    try {
        $key.SetValue($Name, $Value, [Microsoft.Win32.RegistryValueKind]::String)
    } finally {
        $key.Close()
    }
}

function Get-NormalizedPathEntry {
    param([Parameter(Mandatory = $true)][string]$Path)
    return $Path.Trim().Trim('"').Replace('/', '\').TrimEnd('\').ToUpperInvariant()
}

function Add-UserPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey("Environment")
    try {
        $current = [string]$key.GetValue("Path", "")
        $needle = Get-NormalizedPathEntry -Path $Path
        foreach ($segment in ($current -split ';')) {
            if ($segment -and (Get-NormalizedPathEntry -Path $segment) -eq $needle) {
                return $false
            }
        }

        if ([string]::IsNullOrWhiteSpace($current)) {
            $updated = $Path
        } elseif ($current.EndsWith(";")) {
            $updated = "$current$Path"
        } else {
            $updated = "$current;$Path"
        }

        $key.SetValue("Path", $updated, [Microsoft.Win32.RegistryValueKind]::ExpandString)
        return $true
    } finally {
        $key.Close()
    }
}

$OpenCommand = "`"$ExePath`" `"%1`""
Set-RegistryDefault "Software\Classes\magnet" "URL:Magnet Protocol"
Set-RegistryValue "Software\Classes\magnet" "URL Protocol" ""
Set-RegistryDefault "Software\Classes\magnet\DefaultIcon" "`"$IconPath`",0"
Set-RegistryDefault "Software\Classes\magnet\shell\open\command" $OpenCommand

Set-RegistryDefault "Software\Classes\.torrent" "superseedr.torrent"
Set-RegistryValue "Software\Classes\.torrent" "Content Type" "application/x-bittorrent"
Set-RegistryDefault "Software\Classes\superseedr.torrent" "Torrent File (superseedr)"
Set-RegistryDefault "Software\Classes\superseedr.torrent\DefaultIcon" "`"$IconPath`",0"
Set-RegistryDefault "Software\Classes\superseedr.torrent\shell\open\command" $OpenCommand

Set-RegistryValue "Software\Classes\Applications\superseedr.exe" "FriendlyAppName" "superseedr"
Set-RegistryDefault "Software\Classes\Applications\superseedr.exe\SupportedTypes" ""
Set-RegistryValue "Software\Classes\Applications\superseedr.exe\SupportedTypes" ".torrent" ""
Set-RegistryDefault "Software\Classes\Applications\superseedr.exe\shell\open\command" $OpenCommand

$ProgramsDir = [Environment]::GetFolderPath("Programs")
$ShortcutDir = Join-Path $ProgramsDir "superseedr"
New-Item -ItemType Directory -Force -Path $ShortcutDir | Out-Null
$ShortcutPath = Join-Path $ShortcutDir "superseedr.lnk"
$Shell = New-Object -ComObject WScript.Shell
$Shortcut = $Shell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $ExePath
$Shortcut.WorkingDirectory = $InstallDir
$Shortcut.IconLocation = $IconPath
$Shortcut.Save()

$PathChanged = Add-UserPath -Path $InstallDir

Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class SuperseedrShellNotify {
    [DllImport("shell32.dll")]
    public static extern void SHChangeNotify(int wEventId, uint uFlags, IntPtr dwItem1, IntPtr dwItem2);

    [DllImport("user32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern IntPtr SendMessageTimeout(IntPtr hWnd, int Msg, IntPtr wParam, string lParam, int fuFlags, int uTimeout, out IntPtr lpdwResult);
}
"@
if ($PathChanged) {
    $Result = [IntPtr]::Zero
    [SuperseedrShellNotify]::SendMessageTimeout([IntPtr]::new(0xffff), 0x001A, [IntPtr]::Zero, "Environment", 0x0002, 5000, [ref]$Result) | Out-Null
}
[SuperseedrShellNotify]::SHChangeNotify(0x08000000, 0, [IntPtr]::Zero, [IntPtr]::Zero)

Start-Process -FilePath $ExePath -WorkingDirectory $InstallDir
'@ | Set-Content -Path $Path -Encoding UTF8
}

function Build-WithIExpress {
    $iexpress = Get-Command "iexpress.exe" -ErrorAction SilentlyContinue
    if (-not $iexpress) {
        throw "IExpress was not found, and Inno Setup is not available."
    }

    $stageDir = Join-Path $OutputDir "iexpress-stage"
    $installScript = Join-Path $stageDir "install-superseedr.ps1"
    $sedPath = Join-Path $stageDir "superseedr-user.sed"
    $outputName = if ($Flavor -eq "private") {
        "superseedr-private-$Version-x64-setup-local.exe"
    } else {
        "superseedr-$Version-x64-setup-local.exe"
    }
    $targetName = Join-Path $OutputDir $outputName

    if (Test-Path $stageDir) {
        Remove-Item -LiteralPath $stageDir -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $stageDir | Out-Null
    Copy-Item -Force -Path $BinaryPath -Destination (Join-Path $stageDir "superseedr.exe")
    Copy-Item -Force -Path $IconPath -Destination (Join-Path $stageDir "app_icon.ico")
    Write-LocalInstallScript -Path $installScript

    $sed = @"
[Version]
Class=IEXPRESS
SEDVersion=3
[Options]
PackagePurpose=InstallApp
ShowInstallProgramWindow=0
HideExtractAnimation=1
UseLongFileName=1
InsideCompressed=1
CAB_FixedSize=0
CAB_ResvCodeSigning=0
RebootMode=N
InstallPrompt=%InstallPrompt%
DisplayLicense=%DisplayLicense%
FinishMessage=%FinishMessage%
TargetName=%TargetName%
FriendlyName=%FriendlyName%
AppLaunched=%AppLaunched%
PostInstallCmd=%PostInstallCmd%
AdminQuietInstCmd=%AdminQuietInstCmd%
UserQuietInstCmd=%UserQuietInstCmd%
SourceFiles=SourceFiles
[Strings]
InstallPrompt=
DisplayLicense=
FinishMessage=
TargetName=$targetName
FriendlyName=superseedr user installer
AppLaunched=powershell.exe -NoProfile -ExecutionPolicy Bypass -File install-superseedr.ps1
PostInstallCmd=<None>
AdminQuietInstCmd=powershell.exe -NoProfile -ExecutionPolicy Bypass -File install-superseedr.ps1
UserQuietInstCmd=powershell.exe -NoProfile -ExecutionPolicy Bypass -File install-superseedr.ps1
FILE0=superseedr.exe
FILE1=app_icon.ico
FILE2=install-superseedr.ps1
[SourceFiles]
SourceFiles0=$stageDir\
[SourceFiles0]
%FILE0%=
%FILE1%=
%FILE2%=
"@
    $sed | Set-Content -Path $sedPath -Encoding ASCII

    Write-Host "Running: $($iexpress.Source) /N $sedPath"
    & $iexpress.Source /N $sedPath
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Find-CscCompiler {
    $candidates = @()
    $command = Get-Command "csc.exe" -ErrorAction SilentlyContinue
    if ($command) {
        $candidates += $command.Source
    }
    $vsRoot = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools"
    if (Test-Path $vsRoot) {
        $candidates += Get-ChildItem $vsRoot -Recurse -Filter "csc.exe" -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty FullName
    }
    return $candidates | Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1
}

function Build-WithCsc {
    $csc = Find-CscCompiler
    if (-not $csc) {
        throw "C# compiler (csc.exe) was not found, and Inno Setup is not available."
    }

    $stageDir = Join-Path $OutputDir "csc-stage"
    $outputName = if ($Flavor -eq "private") {
        "superseedr-private-$Version-x64-setup-local.exe"
    } else {
        "superseedr-$Version-x64-setup-local.exe"
    }
    $targetName = Join-Path $OutputDir $outputName

    if (Test-Path $stageDir) {
        Remove-Item -LiteralPath $stageDir -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $stageDir | Out-Null

    $program = @'
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Win32;

static class Program
{
    [DllImport("shell32.dll")]
    private static extern void SHChangeNotify(int wEventId, uint uFlags, IntPtr dwItem1, IntPtr dwItem2);

    [DllImport("user32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    private static extern IntPtr SendMessageTimeout(IntPtr hWnd, uint Msg, UIntPtr wParam, string lParam, uint fuFlags, uint uTimeout, out UIntPtr lpdwResult);

    private const int HWND_BROADCAST = 0xffff;
    private const uint WM_SETTINGCHANGE = 0x001A;
    private const uint SMTO_ABORTIFHUNG = 0x0002;

    static int Main()
    {
        try
        {
            string localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            string installDir = Path.Combine(localAppData, "Programs", "superseedr");
            string exePath = Path.Combine(installDir, "superseedr.exe");
            string iconPath = Path.Combine(installDir, "app_icon.ico");

            Directory.CreateDirectory(installDir);
            ExtractResource("superseedr.exe", exePath);
            ExtractResource("app_icon.ico", iconPath);

            string openCommand = $"\"{exePath}\" \"%1\"";
            SetDefault(@"Software\Classes\magnet", "URL:Magnet Protocol");
            SetValue(@"Software\Classes\magnet", "URL Protocol", "");
            SetDefault(@"Software\Classes\magnet\DefaultIcon", $"\"{iconPath}\",0");
            SetDefault(@"Software\Classes\magnet\shell\open\command", openCommand);

            SetDefault(@"Software\Classes\.torrent", "superseedr.torrent");
            SetValue(@"Software\Classes\.torrent", "Content Type", "application/x-bittorrent");
            SetDefault(@"Software\Classes\superseedr.torrent", "Torrent File (superseedr)");
            SetDefault(@"Software\Classes\superseedr.torrent\DefaultIcon", $"\"{iconPath}\",0");
            SetDefault(@"Software\Classes\superseedr.torrent\shell\open\command", openCommand);

            SetValue(@"Software\Classes\Applications\superseedr.exe", "FriendlyAppName", "superseedr");
            SetValue(@"Software\Classes\Applications\superseedr.exe\SupportedTypes", ".torrent", "");
            SetDefault(@"Software\Classes\Applications\superseedr.exe\shell\open\command", openCommand);

            CreateStartMenuShortcut(exePath, iconPath, installDir);
            if (AddToUserPath(installDir))
            {
                BroadcastEnvironmentChange();
                Console.WriteLine("Added superseedr to the user PATH. Restart open terminals before running superseedr by name.");
            }
            SHChangeNotify(0x08000000, 0, IntPtr.Zero, IntPtr.Zero);

            Process.Start(new ProcessStartInfo(exePath) { WorkingDirectory = installDir, UseShellExecute = true });
            Console.WriteLine($"Installed superseedr to {installDir}");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
            return 1;
        }
    }

    private static void ExtractResource(string name, string destination)
    {
        Assembly assembly = Assembly.GetExecutingAssembly();
        using (Stream source = assembly.GetManifestResourceStream(name))
        {
            if (source == null)
            {
                throw new InvalidOperationException("Missing embedded resource: " + name);
            }
            using (FileStream output = File.Create(destination))
            {
                source.CopyTo(output);
            }
        }
    }

    private static void SetDefault(string subKey, string value)
    {
        using (RegistryKey key = Registry.CurrentUser.CreateSubKey(subKey))
        {
            if (key == null)
            {
                throw new InvalidOperationException("Failed to create HKCU\\" + subKey);
            }
            key.SetValue("", value, RegistryValueKind.String);
        }
    }

    private static void SetValue(string subKey, string name, string value)
    {
        using (RegistryKey key = Registry.CurrentUser.CreateSubKey(subKey))
        {
            if (key == null)
            {
                throw new InvalidOperationException("Failed to create HKCU\\" + subKey);
            }
            key.SetValue(name, value, RegistryValueKind.String);
        }
    }

    private static bool AddToUserPath(string installDir)
    {
        using (RegistryKey key = Registry.CurrentUser.CreateSubKey(@"Environment"))
        {
            if (key == null)
            {
                throw new InvalidOperationException(@"Failed to create HKCU\Environment");
            }

            string current = key.GetValue("Path", "") as string ?? "";
            if (PathListContains(current, installDir))
            {
                return false;
            }

            string updated;
            if (string.IsNullOrWhiteSpace(current))
            {
                updated = installDir;
            }
            else if (current.EndsWith(";"))
            {
                updated = current + installDir;
            }
            else
            {
                updated = current + ";" + installDir;
            }

            key.SetValue("Path", updated, RegistryValueKind.ExpandString);
            return true;
        }
    }

    private static bool PathListContains(string pathList, string entry)
    {
        string needle = NormalizePathEntry(entry);
        foreach (string segment in pathList.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            if (NormalizePathEntry(segment) == needle)
            {
                return true;
            }
        }
        return false;
    }

    private static string NormalizePathEntry(string value)
    {
        return value.Trim().Trim('"').Replace('/', '\\').TrimEnd('\\').ToUpperInvariant();
    }

    private static void BroadcastEnvironmentChange()
    {
        UIntPtr result;
        SendMessageTimeout(new IntPtr(HWND_BROADCAST), WM_SETTINGCHANGE, UIntPtr.Zero, "Environment", SMTO_ABORTIFHUNG, 5000, out result);
    }

    private static void CreateStartMenuShortcut(string exePath, string iconPath, string installDir)
    {
        string programs = Environment.GetFolderPath(Environment.SpecialFolder.Programs);
        string shortcutDir = Path.Combine(programs, "superseedr");
        Directory.CreateDirectory(shortcutDir);

        Type shellType = Type.GetTypeFromProgID("WScript.Shell");
        if (shellType == null)
        {
            return;
        }

        object shell = Activator.CreateInstance(shellType);
        object shortcut = shellType.InvokeMember(
            "CreateShortcut",
            BindingFlags.InvokeMethod,
            null,
            shell,
            new object[] { Path.Combine(shortcutDir, "superseedr.lnk") });
        Type shortcutType = shortcut.GetType();
        shortcutType.InvokeMember("TargetPath", BindingFlags.SetProperty, null, shortcut, new object[] { exePath });
        shortcutType.InvokeMember("WorkingDirectory", BindingFlags.SetProperty, null, shortcut, new object[] { installDir });
        shortcutType.InvokeMember("IconLocation", BindingFlags.SetProperty, null, shortcut, new object[] { iconPath });
        shortcutType.InvokeMember("Save", BindingFlags.InvokeMethod, null, shortcut, null);
    }
}
'@

    $programPath = Join-Path $stageDir "Program.cs"
    Set-Content -Path $programPath -Value $program -Encoding UTF8

    Write-Host "Running: $csc /nologo /target:exe /platform:x64 /optimize+ /win32icon:$IconPath /out:$targetName /resource:$BinaryPath,superseedr.exe /resource:$IconPath,app_icon.ico $programPath"
    & $csc /nologo /target:exe /platform:x64 /optimize+ "/win32icon:$IconPath" "/out:$targetName" "/resource:$BinaryPath,superseedr.exe" "/resource:$IconPath,app_icon.ico" $programPath
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

if (-not $Version) {
    $Version = Get-ManifestVersion
}

Push-Location $RepoRoot
try {
    if (-not $SkipBuild) {
        $cargoArgs = @("build", "--release")
        if ($Flavor -eq "private") {
            $cargoArgs += "--no-default-features"
        }
        Write-Host "Running: cargo $($cargoArgs -join ' ')"
        & cargo @cargoArgs
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    }

    if (-not (Test-Path $BinaryPath)) {
        throw "Expected release binary not found: $BinaryPath"
    }
    if (-not (Test-Path $IconPath)) {
        throw "Expected icon not found: $IconPath"
    }

    New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

    $iscc = Find-InnoCompiler
    if ($Backend -eq "inno" -and -not $iscc) {
        throw "Inno Setup 6 compiler (ISCC.exe) was not found."
    }

    $csc = Find-CscCompiler

    if (($Backend -eq "inno") -or ($Backend -eq "auto" -and $iscc)) {
        Build-WithInno -Iscc $iscc
    } elseif (($Backend -eq "csc") -or ($Backend -eq "auto" -and $csc)) {
        if ($Backend -eq "auto") {
            Write-Host "Inno Setup was not found; falling back to local C# installer."
        }
        Build-WithCsc
    } else {
        if ($Backend -eq "auto") {
            Write-Host "Inno Setup was not found; falling back to local IExpress installer."
        }
        Build-WithIExpress
    }

    $latest = Get-ChildItem -Path $OutputDir -Filter "*.exe" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $latest) {
        throw "Installer build completed but no .exe was found in $OutputDir"
    }

    Write-Host "Built installer: $($latest.FullName)"
} finally {
    Pop-Location
}
