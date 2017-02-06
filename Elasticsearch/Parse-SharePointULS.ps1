param($environment)

begin
{ 
}
process
{
    $source = $_


    $reader = [System.IO.File]::OpenText($source)
    
    $currentRow=0;

    $computername=($_ | split-path -Leaf).Substring(0,($_ | split-path -Leaf).IndexOf('-'))
    $exceptions=Get-Content ..\SharePoint\SharePointULSLog_Exclusions.txt

    while ( !$reader.EndOfStream) 
    {
        $line = $reader.ReadLine()
        $data=$line -split "`t"
        
        $currentRow++
        if($currentRow -gt 1)
        {
            $level=$data[6].Trim()
            $eventId=$data[5].Trim()
            $category=$data[4].Trim()

            if($level -ne "Information")
            {
                
                $isExcluded=$exceptions.Contains("$eventId,$level,$category")

                if(-not $isExcluded)
                {
                    $eventTimestampInUTC=(Get-date ($data[0].Trim()).Replace('*','')).ToUniversalTime()

                    $lineProperties=@{}
                    $lineProperties.Add("ComputerName",$computername)
                    $lineProperties.Add("Timestamp",(Get-date $eventTimestampInUTC -Format yyyyMMdd) + "T" + (Get-date $eventTimestampInUTC -Format hhmmss) + ".000Z")
                    $lineProperties.Add("Process",$data[1].Trim())
                    $lineProperties.Add("Area",$data[3].Trim())
                    $lineProperties.Add("Category",$category)
                    $lineProperties.Add("EventID",$eventId)
                    $lineProperties.Add("Level",$level)
                    $lineProperties.Add("Message",$data[7].Trim())
                    $lineProperties.Add("Correlation",$data[8].Trim())
                    $lineProperties.Add("Environment",$environment)

                    $lineProperties
                } 
            }
        }

    }

    $reader.close()

    
    
    

    

}
