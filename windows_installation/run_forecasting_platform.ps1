Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-NativeCommand() {
    # Wrapper to run a command, and automatically throw an error if the exit code is non-zero.

    if ($args.Count -eq 0) {
        throw "Must supply some arguments."
    }

    $command = $args[0]
    $commandArgs = @()
    if ($args.Count -gt 1) {
        $commandArgs = $args[1..($args.Count - 1)]
    }

    Write-Debug "Invoked command: $command"
    Write-Debug "Invoked command args: $commandArgs"

    & $command @commandArgs
    $result = $LastExitCode

    if ($result -ne 0) {
        throw "$command $commandArgs exited with code $result."
    }
}

Push-Location (Split-Path (Get-Variable MyInvocation -Scope Script).Value.MyCommand.Path)  # Change working directory to script directory

$env:Path="C:\Program Files\Python38;$env:Path"
echo $env:Path

. "../ow-forecasting-venv/Scripts/Activate.ps1"

Invoke-NativeCommand poetry env info
Invoke-NativeCommand poetry env list
Invoke-NativeCommand forecasting_platform @args
