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

function Invoke-Replay {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Worktree,

        [Parameter(Mandatory = $true)]
        [string]$TargetDir,

        [Parameter(Mandatory = $true)]
        [string[]]$Filters
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
                    $lines = & cargo test $filter -- --nocapture 2>&1
                }
                finally {
                    $ErrorActionPreference = $previousErrorAction
                }
                $exitCode = $LASTEXITCODE
                foreach ($line in $lines) {
                    $output.Add([string]$line)
                }
                if ($exitCode -ne 0) {
                    throw "cargo test $filter failed in $Worktree with exit code $exitCode"
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
        if ($line -match '^SUPERSEEDR_DHT_REPLAY_BEGIN (.+)$') {
            if ($null -ne $currentName) {
                throw "nested replay trace marker for $currentName"
            }
            $currentName = $Matches[1]
            $currentLines.Clear()
            continue
        }

        if ($line -match '^SUPERSEEDR_DHT_REPLAY_END (.+)$') {
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
$runId = Get-Date -Format "yyyyMMdd_HHmmss"
$runRoot = Join-Path $repoRootPath "tmp\dht-replay-diff\$runId"
$baselineWorktree = Join-Path $runRoot "baseline"
$currentWorktree = Join-Path $runRoot "current"
$baselineTarget = Join-Path $runRoot "target-baseline"
$currentTarget = Join-Path $runRoot "target-current"
$baselineTrace = Join-Path $runRoot "baseline.trace.txt"
$currentTrace = Join-Path $runRoot "current.trace.txt"

New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

try {
    Invoke-Git -Args @("-C", $repoRootPath, "worktree", "add", "--detach", $baselineWorktree, $BaselineRef)
    Invoke-Git -Args @("-C", $repoRootPath, "worktree", "add", "--detach", $currentWorktree, $CurrentRef)

    $baselineOutput = Invoke-Replay -Worktree $baselineWorktree -TargetDir $baselineTarget -Filters $TestFilters
    $currentOutput = Invoke-Replay -Worktree $currentWorktree -TargetDir $currentTarget -Filters $TestFilters

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
