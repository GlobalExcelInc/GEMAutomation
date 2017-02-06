Function Add-ElasticsearchDocument($serverName,[string]$indexName,$documentType)
{
    begin
    {
        Import-Module -Force ..\Utilities\libJSON.psm1
        $indexName=$indexName.ToLower()
        $enc = [ system.Text.Encoding ]::UTF8
        
    }
    process
    {
        $body = '{'
        $properties=$_
        $propertyCount=$properties.Count
        $currentPropertyIndex=1
        foreach($property in $properties.GetEnumerator())
        {
            $escapedValue=$property.Value #(Get-EscapedJSONBody -originalBody $property.Value)
            $body+='"' + $property.Key.ToLower() + '":'
            <#if($escapedValue -ne "null")
            {
                 $body+='"'+$escapedValue + '"'
            }
            else
            {
                $body+=$escapedValue    
            }#>
            $body+=$property.Value | ConvertTo-Json
            if($currentPropertyIndex -ne  $propertyCount)
            {
                $body+=","    
            }

            $currentPropertyIndex++
        }


        $body+='}'

        #$body=$body | ConvertTo-Json

        try
        {
            Invoke-WebRequest -Method Post -Uri "http://$serverName`:9200/$indexName/$documentType/" -Body $enc.GetBytes($body) -ContentType "application/json;charset=utf-8"  | Out-Null
            Write-Host "Inserted document in Elasticsearch"
        }
        catch
        {
            ($_.ErrorDetails.Message | ConvertFrom-Json | select error).error
        }
    }
}

Function Add-SQLQueryResultAsDocumentBatch($sqlServerName,$databaseName,$query,[hashtable]$queryParameters,$elasticsearchServerName,$indexName,$documentType,$batchSize,[bool]$trackCheckPoint,$checkPointParameterName)
{
   
    $varcharDataType=[System.Data.SqlDbType]::VarChar
    Add-Type -AssemblyName System.Web.Extensions
    $jsonSerializer=New-Object -TypeName System.Web.Script.Serialization.JavaScriptSerializer
    $body=New-Object -TypeName System.Text.StringBuilder

    $sqlConn = $sqlConn = new-object ("Data.SqlClient.SqlConnection") "Data Source=$sqlServerName;Initial Catalog=$databaseName;Integrated Security=True;MultipleActiveResultSets=True"

    if($sqlConn.State -ne "Open")
    {
        $sqlConn.Open()
    }

    
    
    $queryCommand = New-Object Data.SqlClient.SqlCommand $query,$sqlConn

    foreach($parameter in $queryParameters.GetEnumerator())
    {
        $queryCommand.Parameters.Add((New-Object -TypeName System.Data.SqlClient.SqlParameter -ArgumentList ("@"+$parameter.Key), $varcharDataType)) | Out-Null
        $queryCommand.Parameters[("@"+$parameter.Key)].Value=$parameter.Value
    }

    $queryCommand.CommandTimeout=10000000
    $reader=$queryCommand.ExecuteReader()
    $batch=@()
    [bool]$recordInitialized=$false
    $record=@{}
    $currentBatchSize=0


    while($reader.Read())
    {
        
        $null = $body.Append('{ "index" : { "_index" : "'+$indexName+'", "_type" : "'+$documentType+'"}}'+"`n")
        $null = $body.Append('{')
                
        $propertyCount=$reader.FieldCount
        $currentPropertyIndex=1
        for($i=0;$i -lt $reader.FieldCount;$i++)
        {
            
            $null = $body.Append('"' + $reader.GetName($i).ToLower() + '":')
           
            $null = $body.Append($jsonSerializer.Serialize($reader[$i]))
            if($currentPropertyIndex -ne  $propertyCount)
            {
                $null = $body.Append(",")
            }

            $currentPropertyIndex++
        }


        $null = $body.Append('}'+"`n")
        $currentBatchSize++

        if($currentBatchSize -eq $batchSize)
        {
            
            Add-ElasticSearchDocumentBatch -serverName $elasticsearchServerName -indexName $indexName -documentType $documentType -batchBody $body.ToString() -batchSize $currentBatchSize
            $currentBatchSize=0
            $null = $body.Clear()
        }
    }
    if($currentBatchSize  -gt 0)
    {
        Add-ElasticSearchDocumentBatch -serverName $elasticsearchServerName -indexName $indexName -documentType $documentType -batchBody $body -batchSize $currentBatchSize
    }

    $sqlConn.Close()

    if($trackCheckPoint -eq $true)
    {
        Set-Content -Path .\Checkpoints\$sqlServerName`_$indexname`_checkpoint.txt -Value ($queryParameters[$checkPointParameterName])  
    }
}

Function Partition-ElasticsearchDocument([hashtable[]]$documents,$partitionKey,$indexPatitionType)
{
    $documentsPartitions=@{}
    foreach($document in $documents.GetEnumerator())
    {
        switch($indexPartitionType)
        {
            "Daily" 
            { 
                $documentPartition=$document[$partitionKey].Substring(0,4) + "." + $document[$partitionKey].Substring(4,2) + "." + $document[$partitionKey].Substring(6,2) 
                if($documentsPartitions[$documentPartition] -eq $null)
                {
                    $documentsPartitions[$documentPartition]=@()   
                }
                $documentsPartitions[$documentPartition]+=$document
            }
            "Monthly"  
            {
                $documentPartition=$document[$partitionKey].Substring(0,4) + "." + $document[$partitionKey].Substring(4,2)
                if($documentsPartitions[$documentPartition] -eq $null)
                {
                    $documentsPartitions[$documentPartition]=@()   
                }
                $documentsPartitions[$documentPartition]+=$document
            }
            default {$indexName=$baseIndexName}
        } 
    }
    $documentsPartitions 
}

Function Create-ElasticsearchDocumentBatch([hashtable[]]$documents,$batchSize,$indexName)
{
    $batches=@()
    $currentBatchSize=0
    if($null -ne $documents)
    {
        if($documents.Count -gt 0)
        {
            Add-Type -AssemblyName System.Web.Extensions
            $jsonSerializer=New-Object -TypeName System.Web.Script.Serialization.JavaScriptSerializer
            $body=New-Object -TypeName System.Text.StringBuilder

            
            foreach($document in $documents.GetEnumerator())
            {
                        
                    $null = $body.Append('{ "index" : { "_index" : "'+$indexName+'", "_type" : "'+$documentType+'"}}'+"`n")
                    $null = $body.Append('{')
                
                    $propertyCount=$document.Count
                    $currentPropertyIndex=1
                    foreach($property in $document.GetEnumerator())
                    {
                        $null = $body.Append('"' + $property.Key.ToLower() + '":')
           
                        $null = $body.Append($jsonSerializer.Serialize($property.Value))
                        if($currentPropertyIndex -ne  $propertyCount)
                        {
                            $null = $body.Append(",")
                        }

                        $currentPropertyIndex++
 
                    }

         

                $null = $body.Append('}'+"`n")
                $currentBatchSize++

                if($currentBatchSize -eq $batchSize)
                {
            
                    $batchBody=$body.ToString()
                    $batches+=$batchBody
                    $null = $body.Clear()
                    $currentBatchSize=0
                }
            }
            
            if($currentBatchSize  -gt 0)
            {
                $batchBody=$body.ToString()
                $batches+=$batchBody
                $null = $body.Clear()   
            }  
            
        }
        $batches
    }    
}


Function Add-ElasticSearchDocumentBatch($serverName,[string]$indexName,$documentType,[hashtable[]]$documents,[string]$batchBody,$batchSize,$partitionKey,$indexPartitionType,$indexDefinitionPath)
{
    begin
    {
        Import-Module -Force ..\Utilities\libJSON.psm1
        $indexName=$indexName.ToLower()
        $enc = [ system.Text.Encoding ]::UTF8
        
    }
    process
    {
        
       if($null -ne $documents)
       {
            $documentsPartitions=Partition-ElasticsearchDocument -documents $documents -partitionKey $partitionKey -indexPatitionType $indexPartitionType

            foreach($documentsPartition in $documentsPartitions.GetEnumerator())
            {
                if($indexPartitionType -ne $null -and $indexDefinitionPath -ne $null)
                {
                    $partitionIndexName=$indexName+"-"+$documentsPartition.Key.ToString()
                    Assert-ElasticsearchIndex -serverName $serverName -indexName $partitionIndexName -indexDefinitionPath $indexDefinitionPath
                    
                }
                else
                {
                    $partitionIndexName=$indexName
                }

                $batches=Create-ElasticsearchDocumentBatch -documents $documentsPartition.Value -batchSize $batchSize -indexName $partitionIndexName

                foreach($batch in $batches)
                {
                    try
                    {
                        Invoke-WebRequest -Method Post -Uri "http://$serverName`:9200/$partitionIndexName/$documentType/_bulk" -Body $enc.GetBytes($batch) -ContentType "application/json;charset=utf-8" -UseBasicParsing | Out-Null
                        Write-Host "Inserted document batch in Elasticsearch"
                    }
                    catch
                    {
                        ($_.ErrorDetails.Message | ConvertFrom-Json | select error).error
                    }
                }
            }
        }
        else
        {
            $body=$batchBody

            if($null -ne $body)
            {
                try
                {
                    
                    Invoke-WebRequest -Method Post -Uri "http://$serverName`:9200/$indexName/$documentType/_bulk" -Body $enc.GetBytes($body) -ContentType "application/json;charset=utf-8" -UseBasicParsing | Out-Null
                    Write-Host "Inserted document batch of"$batchSize" in Elasticsearch"
                }
                catch
                {
                    ($_.ErrorDetails.Message | ConvertFrom-Json | select error).error
                }
            }
        }
        

    }
}

Function Assert-ElasticsearchIndex($serverName,$indexName,$indexDefinitionPath)
{
    try
    {
        $existingIndex=Get-ElasticsearchIndex -serverName $serverName -indexName $indexName
    }
    catch
    {
        Write-Host "Index $indexName doesn't exist, attempting to create"
        $result=Add-ElasticsearchIndex -serverName $serverName -indexName $indexName -indexDefinition (Get-Content $indexDefinitionPath)
        if($result.StatusCode -eq 200)
        {
            Write-Host "Created $indexName"
        }
        else
        {
            Write-Host "Could not create $indexName"
        }
    }   
}

Function Assert-ElasticsearchIndexPartition($serverName,$baseIndexName,$indexPartitionType,$indexDefinitionPath)
{
    $indexName=""
    switch($indexPartitionType)
    {
        "Daily" 
        {
            $currentDate=Get-Date -Format "-yyyyMMdd"
            $indexName=$baseIndexName+$currentDate
        }
        "Monthly"  
        {
            $currentDate=Get-Date -Format "-yyyyMM"
            $indexName=$baseIndexName+$currentDate
        }
        default {$indexName=$baseIndexName}
    }
    
    try
    {
        $existingIndex=Get-ElasticsearchIndex -serverName $serverName -indexName $indexName
    }
    catch
    {
        Write-Host "Partition $indexName for $baseIndexName doesn't exist, attempting to create"
        $result=Add-ElasticsearchIndex -serverName $serverName -indexName $indexName -indexDefinition (Get-Content $indexDefinitionPath)
        if($result.StatusCode -eq 200)
        {
            Write-Host "Created $indexName"
        }
        else
        {
            Write-Host "Could not create $indexName"
        }
    }

}

Function Add-ElasticsearchIndex($serverName,[string]$indexName,$indexDefinition)
{
    Invoke-WebRequest -Method Put -Uri "http://$serverName`:9200/$indexName" -Body $indexDefinition -ContentType "application/json"
}

Function Get-ElasticsearchIndex($serverName,$indexName)
{
    Invoke-WebRequest -Method Get -Uri "http://$serverName`:9200/$indexName"    
}

Function Remove-ElasticsearchIndex($serverName,[string]$indexName)
{
    Invoke-WebRequest -Method Delete -Uri "http://$serverName`:9200/$indexName"    
}

Function Add-ElasticSearchIncrementalSQLQueryDocumentBatch()
{

}