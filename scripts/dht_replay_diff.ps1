param(
    [Parameter(Mandatory = $true)]
    [string]$BaselineRef,

    [string]$CurrentRef = "HEAD",

    [string]$RepoRoot = "",

    [string[]]$TestFilters = @(
        "dht::service::planner::replay_tests",
        "dht::service::replay_tests",
        "dht::service::runtime_command_replay_tests"
    ),

    [ValidateSet("All", "Planner")]
    [string]$HarnessScope = "All",

    [switch]$BackportReplayHarness,

    [string[]]$HarnessCommits = @(),

    [switch]$KeepWorktrees
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $scriptRoot = $PSScriptRoot
    if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
        $scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    }
    $RepoRoot = (Resolve-Path (Join-Path $scriptRoot "..")).Path
}

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )
    & git @Args
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Args -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Invoke-GitOutput {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )
    $output = & git @Args
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Args -join ' ') failed with exit code $LASTEXITCODE"
    }
    return @($output)
}

function Test-WorktreeContainsCommit {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Worktree,

        [Parameter(Mandatory = $true)]
        [string]$Commit
    )

    & git -C $Worktree merge-base --is-ancestor $Commit HEAD | Out-Null
    return $LASTEXITCODE -eq 0
}

function Resolve-HarnessPaths {
    param(
        [ValidateSet("All", "Planner")]
        [string]$Scope = "All"
    )

    if ($Scope -eq "Planner") {
        return @("src/dht/service/planner/replay_tests.rs")
    }

    return @(
        "src/dht/service/planner/replay_tests.rs",
        "src/dht/service/replay_tests.rs",
        "src/dht/service/runtime_command_replay_tests.rs"
    )
}

function Resolve-HarnessCommits {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Repository,

        [Parameter(Mandatory = $true)]
        [string[]]$Paths
    )

    $commits = New-Object System.Collections.Generic.List[string]
    $gitArgs = @(
        "-C", $Repository,
        "log",
        "--format=%H",
        "--reverse",
        "--"
    ) + $Paths
    $pathCommits = @(Invoke-GitOutput -Args $gitArgs)
    foreach ($pathCommit in $pathCommits) {
        $commit = [string]$pathCommit
        if (-not $commits.Contains($commit)) {
            $commits.Add($commit)
        }
    }
    if ($commits.Count -eq 0) {
        throw "could not find replay harness commits for selected paths"
    }
    return @($commits.ToArray())
}

function Add-ReplayHarness {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Worktree,

        [Parameter(Mandatory = $true)]
        [string[]]$Commits,

        [Parameter(Mandatory = $true)]
        [string[]]$Paths
    )

    foreach ($commit in $Commits) {
        if (Test-WorktreeContainsCommit -Worktree $Worktree -Commit $commit) {
            continue
        }

        $patch = & git -C $Worktree show --format= --binary $commit -- @Paths
        if ($LASTEXITCODE -ne 0) {
            throw "failed to read replay harness patch $commit for $Worktree"
        }
        if ($patch.Count -eq 0) {
            continue
        }

        $previousErrorAction = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $applyOutput = $patch | & git -C $Worktree apply --index --3way --whitespace=nowarn 2>&1
        }
        finally {
            $ErrorActionPreference = $previousErrorAction
        }
        if ($LASTEXITCODE -ne 0) {
            $details = ($applyOutput | ForEach-Object { [string]$_ }) -join [Environment]::NewLine
            throw "failed to backport replay harness patch $commit into $Worktree$([Environment]::NewLine)$details"
        }
    }
}

function Add-TextBeforeMarker {
    param(
        [Parameter(Mandatory = $true)]
        [string]$File,

        [Parameter(Mandatory = $true)]
        [string]$Needle,

        [Parameter(Mandatory = $true)]
        [string]$Marker,

        [Parameter(Mandatory = $true)]
        [string]$Block
    )

    $content = (Get-Content -Raw -LiteralPath $File).Replace("`r`n", "`n")
    if ($content.Contains($Needle)) {
        return
    }

    $markerIndex = $content.IndexOf($Marker)
    if ($markerIndex -lt 0) {
        throw "could not find replay harness insertion marker in $File"
    }

    $updated = $content.Insert($markerIndex, $Block)
    Set-Content -LiteralPath $File -Value $updated -NoNewline -Encoding utf8
}

function Ensure-ReplayHarnessModules {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Worktree,

        [ValidateSet("All", "Planner")]
        [string]$Scope = "All"
    )

    Add-TextBeforeMarker `
        -File (Join-Path $Worktree "src\dht\service\planner.rs") `
        -Needle 'mod replay_tests;' `
        -Marker '#[derive(Debug, Clone, Copy, Default)]' `
        -Block @"
#[cfg(test)]
#[path = "planner/replay_tests.rs"]
mod replay_tests;

"@

    if ($Scope -eq "Planner") {
        return
    }

    Add-TextBeforeMarker `
        -File (Join-Path $Worktree "src\dht\service.rs") `
        -Needle 'mod runtime_command_replay_tests;' `
        -Marker @"
#[cfg(test)]
#[path = "service/api_tests.rs"]
"@ `
        -Block @"
#[cfg(test)]
#[path = "service/replay_tests.rs"]
mod replay_tests;

#[cfg(test)]
#[path = "service/runtime_command_replay_tests.rs"]
mod runtime_command_replay_tests;

"@
}

function Invoke-Replay {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Worktree,

        [Parameter(Mandatory = $true)]
        [string]$TargetDir,

        [Parameter(Mandatory = $true)]
        [string[]]$Filters,

        [Parameter(Mandatory = $true)]
        [string]$LogPath
    )

    $oldReplay = $env:SUPERSEEDR_DHT_REPLAY_PRINT
    $oldTarget = $env:CARGO_TARGET_DIR
    try {
        $env:SUPERSEEDR_DHT_REPLAY_PRINT = "1"
        $env:CARGO_TARGET_DIR = $TargetDir
        $output = New-Object System.Collections.Generic.List[string]

        Push-Location $Worktree
        try {
            foreach ($filter in $Filters) {
                $previousErrorAction = $ErrorActionPreference
                $ErrorActionPreference = "Continue"
                try {
                    $lines = & cargo test $filter -- --nocapture --test-threads=1 2>&1
                }
                finally {
                    $ErrorActionPreference = $previousErrorAction
                }
                $exitCode = $LASTEXITCODE
                foreach ($line in $lines) {
                    $output.Add([string]$line)
                }
                Set-Content -LiteralPath $LogPath -Value ($output -join [Environment]::NewLine) -Encoding utf8
                if ($exitCode -ne 0) {
                    throw "cargo test $filter failed in $Worktree with exit code $exitCode; log: $LogPath"
                }
            }
        }
        finally {
            Pop-Location
        }

        return Extract-ReplayTraces -Lines @($output.ToArray())
    }
    finally {
        $env:SUPERSEEDR_DHT_REPLAY_PRINT = $oldReplay
        $env:CARGO_TARGET_DIR = $oldTarget
    }
}

function Extract-ReplayTraces {
    param(
        [AllowEmptyCollection()]
        [object[]]$Lines
    )

    $traces = New-Object System.Collections.Generic.List[string]
    $currentName = $null
    $currentLines = New-Object System.Collections.Generic.List[string]

    foreach ($rawLine in $Lines) {
        $line = [string]$rawLine
        if ($line -match 'SUPERSEEDR_DHT_REPLAY_BEGIN (\S+)') {
            if ($null -ne $currentName) {
                throw "nested replay trace marker for $currentName"
            }
            $currentName = $Matches[1]
            $currentLines.Clear()
            continue
        }

        if ($line -match 'SUPERSEEDR_DHT_REPLAY_END (\S+)') {
            if ($null -eq $currentName) {
                throw "replay trace end without begin"
            }
            if ($Matches[1] -ne $currentName) {
                throw "replay trace end marker $($Matches[1]) did not match $currentName"
            }
            $traces.Add("TRACE $currentName")
            foreach ($traceLine in $currentLines) {
                $traces.Add($traceLine)
            }
            $traces.Add("")
            $currentName = $null
            $currentLines.Clear()
            continue
        }

        if ($null -ne $currentName) {
            $currentLines.Add($line)
        }
    }

    if ($null -ne $currentName) {
        throw "unterminated replay trace marker for $currentName"
    }
    if ($traces.Count -eq 0) {
        throw "no replay traces were emitted; ensure SUPERSEEDR_DHT_REPLAY_PRINT is supported by the selected refs"
    }

    return ($traces -join [Environment]::NewLine)
}

$repoRootPath = (Resolve-Path $RepoRoot).Path
if ($HarnessScope -eq "Planner" -and -not $PSBoundParameters.ContainsKey("TestFilters")) {
    $TestFilters = @("dht::service::planner::replay_tests")
}

$runId = Get-Date -Format "yyyyMMdd_HHmmss"
$runRoot = Join-Path $repoRootPath "tmp\dht-replay-diff\$runId"
$baselineWorktree = Join-Path $runRoot "baseline"
$currentWorktree = Join-Path $runRoot "current"
$baselineTarget = Join-Path $runRoot "target-baseline"
$currentTarget = Join-Path $runRoot "target-current"
$baselineTrace = Join-Path $runRoot "baseline.trace.txt"
$currentTrace = Join-Path $runRoot "current.trace.txt"
$baselineLog = Join-Path $runRoot "baseline.cargo.log"
$currentLog = Join-Path $runRoot "current.cargo.log"
$HarnessPaths = Resolve-HarnessPaths -Scope $HarnessScope

New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

if ($BackportReplayHarness -and $HarnessCommits.Count -eq 0) {
    $HarnessCommits = Resolve-HarnessCommits -Repository $repoRootPath -Paths $HarnessPaths
}

try {
    Invoke-Git -Args @("-C", $repoRootPath, "worktree", "add", "--detach", $baselineWorktree, $BaselineRef)
    Invoke-Git -Args @("-C", $repoRootPath, "worktree", "add", "--detach", $currentWorktree, $CurrentRef)

    if ($BackportReplayHarness) {
        Add-ReplayHarness -Worktree $baselineWorktree -Commits $HarnessCommits -Paths $HarnessPaths
        Add-ReplayHarness -Worktree $currentWorktree -Commits $HarnessCommits -Paths $HarnessPaths
        Ensure-ReplayHarnessModules -Worktree $baselineWorktree -Scope $HarnessScope
        Ensure-ReplayHarnessModules -Worktree $currentWorktree -Scope $HarnessScope
    }

    $baselineOutput = Invoke-Replay -Worktree $baselineWorktree -TargetDir $baselineTarget -Filters $TestFilters -LogPath $baselineLog
    $currentOutput = Invoke-Replay -Worktree $currentWorktree -TargetDir $currentTarget -Filters $TestFilters -LogPath $currentLog

    Set-Content -LiteralPath $baselineTrace -Value $baselineOutput -Encoding utf8
    Set-Content -LiteralPath $currentTrace -Value $currentOutput -Encoding utf8

    if ($baselineOutput -eq $currentOutput) {
        Write-Host "DHT replay traces match."
        Write-Host "Baseline trace: $baselineTrace"
        Write-Host "Current trace:  $currentTrace"
        exit 0
    }

    Write-Host "DHT replay traces differ."
    Write-Host "Baseline trace: $baselineTrace"
    Write-Host "Current trace:  $currentTrace"
    & git diff --no-index -- $baselineTrace $currentTrace
    exit 1
}
finally {
    if (-not $KeepWorktrees) {
        if (Test-Path -LiteralPath $baselineWorktree) {
            & git -C $repoRootPath worktree remove --force $baselineWorktree | Out-Null
        }
        if (Test-Path -LiteralPath $currentWorktree) {
            & git -C $repoRootPath worktree remove --force $currentWorktree | Out-Null
        }
    }
}
