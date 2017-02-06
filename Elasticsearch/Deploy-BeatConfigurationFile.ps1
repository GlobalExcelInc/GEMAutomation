param($templateConfigurationFilePath,$targetedAgentsListFilePath,$elasticsearchHostName)

if($targetedAgentsListFilePath -ne $null)
{
    $targetServers=Import-Csv -Path $targetedAgentsListFilePath
}

foreach($targetServer in $targetServers)
{
    
    $destinationPath="\\" + $targetServer.ServerName +"\c$\Tools\winlogbeat\winlogbeat-5.0.1-windows-x86_64\"

    if(Test-Path -Path $destinationPath)
    {
        Write-Host "Winlogbeat on"$targetServer.ServerName"is not in standard path, please fix"
    }
    else
    {
        $destinationPath="\\" + $targetServer.ServerName +"\c$\Tools\winlogbeat\"

        Write-Host "Copying configuration file on"$targetServer.ServerName
        $configurationFile=Get-Content -Path $templateConfigurationFilePath
        $configurationFile=$configurationFile.Replace("{`$elasticsearchHostName$}",$elasticsearchHostName)
        $configurationFile=$configurationFile.Replace("{`$primaryEnvironmentName$}",$targetServer.PrimaryEnvironmentName)
        $configurationFile=$configurationFile.Replace("{`$primarySystemName$}",$targetServer.PrimarySystemName)
        $configurationFile | Set-Content -Path $destinationPath\winlogbeat.yml -Force


        Write-Host "Restarting winlogbeat on"$targetServer.ServerName
        Get-Service -ComputerName $targetServer.ServerName -Name winlogbeat | Restart-Service
    }
}