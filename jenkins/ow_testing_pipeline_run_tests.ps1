Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# set $LastExitCode so the script doesn't fail in Jenkins if no exit code is returned
$LastExitCode = 0

$Password = ConvertTo-SecureString -String "$( $ENV:REMOTE_PASSWORD )" -AsPlainText -Force
$Credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "$ENV:REMOTE_USER", $Password

function Run-Forecasting-Platform()
{
    try
    {
        Invoke-Command -ComputerName "OWGAWEUW1PAP02" -Credential $Credential -ConfigurationName PAP_service_fsc_tst -ScriptBlock {
            Write-Output "Running forecasting platform: $Using:args"
            cd "C:/Users/PAP_service_fsc_tst"
            powershell -File ./ow-forecasting-install/run_forecasting_platform.ps1 $Using:args 2>&1 | %{ "$_" }
            if ($LastExitCode -ne 0)
            {
                throw "Running forecasting platform $Using:args exited with code $LastExitCode."
            }
        }
    }
    catch
    {
        Write-Host -Foreground Red -Background Black "Remote Command execution failed: ($_)"
        exit 1
    }
    Write-Output "Remote Command execution successful"
}

Run-Forecasting-Platform info

Write-Output "$( Get-Date ) [Run Tests] Setup internal and DSX database ..."
Run-Forecasting-Platform setup-database internal --drop-tables
Run-Forecasting-Platform setup-database dsx-write --drop-tables

Write-Output "$( Get-Date ) [Run Tests] Run forecasts ..."
# run production command (includes cleaning) and verify against database
Run-Forecasting-Platform production --forecast-periods 1 --prediction-start-month 202001 --output-location test-results/production-run/
Run-Forecasting-Platform compare-structure-database

# run backward forecast and verify against expected result files
Run-Forecasting-Platform backward --prediction-end-month 202003 --output-location test-results/backward-run/ --exclude-model-config ModelConfigAccount13 --forecast-periods 9
Run-Forecasting-Platform backward --prediction-end-month 202003 --output-location test-results/backward-run/ --only-model-config ModelConfigAccount13 --forecast-periods 6
Run-Forecasting-Platform compare-results expected_results/ test-results/backward-run/

# run development forecast (includes cleaning and update of actuals) and verify against expected structure
Run-Forecasting-Platform development --forecast-periods 1 --prediction-start-month 202001 --output-location test-results/development-run/
Run-Forecasting-Platform compare-structure test-results/development-run/

# verify if results of production command are still valid
Run-Forecasting-Platform compare-structure-database

Write-Output "$( Get-Date ) [Run Test] Finished"
