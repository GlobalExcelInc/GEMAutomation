param($elasticsearchHostName)

Import-Module -Force .\libElasticsearch.psm1

$servers=Import-Csv ..\SharePoint\SharePointServers.csv

foreach($server in $servers)
{
    $checkpointFilePath=".\Checkpoints\" + $server.ComputerName+ "_sharepointULS_checkpoint.txt"

    if(-not (Test-Path $checkpointFilePath))
    {
        Set-Content -Path $checkpointFilePath -Value "2000-01-01 00:00:00"
    }

    $startingPoint=(Get-Content $checkpointFilePath)
    


    $logFilePath=$server.ULSLogNetworkBasePath + $server.ComputerName + "-*.log"
    $logFiles=Get-ChildItem -Path $logFilePath | Where-Object {$_.LastWriteTime -ge $startingPoint}
    if($logFiles -ne $null)
    {
        $lastLogFileTime=($logFiles | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1).LastWriteTime
        
        
        $logFiles=$logFiles | Where-Object {$_.LastWriteTime -lt $lastLogFileTime}

        if($logFiles -ne $null)
        {
            $endPoint=Get-Date $lastLogFileTime

            foreach($logFile in $logFiles)
            {
                Write-Host "Parsing "$logFile.FullName
                $logEntries=@($logFile.FullName | .\Parse-SharePointULS.ps1 -environment $server.Environment)
        
                if($logEntries.Count -gt 0)
                {
                    Add-ElasticSearchDocumentBatch -serverName $elasticsearchHostName -batchSize 1000 -indexName sharepointulslog -documentType ulslogentry -documents $logEntries -partitionKey "timestamp" -indexPartitionType "Daily" -indexDefinitionPath .\IndexDefinitions\SharePointULS.json
                }
            }

            Set-Content -Path $checkpointFilePath -Value (Get-Date $endPoint -Format "yyyy-MM-dd HH:mm:ss")
        }
        else
        {
            Write-Host "Found new file to import but it's currently in use, skipping"
        }
       
    }
    else
    {
        Write-Host "No new files to import"
    }
}