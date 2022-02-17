Param (
  $server = "",
  $filePath = ".",
  $logList = $null,
  $outFile = ".\perfdata.csv",
  $newFile = $false
)

# regex patterns
$patternJobPipeApp = "Job-ID: (?<jobid>\d+)\s+\[Pipe-ID: (?<pipeid>\d+)]\s+\[App-Type: (?<apptype>\d+)]"
$patternSource = "Stream Source:\s+(?<source>.+)"
$patternStartEnd = "Head duration \(Local\):\s+\[(?<start>\d+,\w+,\d+ \d{2}:\d{2}:\d{2})\s+~\s+(?<end>\d+,\w+,\d+ \d{2}:\d{2}:\d{2})"
$patternReceive = "SDT: Receive Data.*?\s+(?:\d|-)+\s+(?<recvbyt>\d+)\s+(.*?\[.*?]\s+){2}\[.*?](?:\s+\[(?<recvgbph>\d+\.{0,1}\d{0,}) GBPH]){0,1}"
$patternSignature = "SDT-Head: Signature module.*\s+(?:\d|-)+\s+(?<sigbyt>\d+)\s+(.*?\[.*?]\s+){2}\[.*?](?:\s+\[(?<siggbph>\d+\.{0,1}\d{0,}) GBPH]){0,1}"
$patternNetTransfer = "SDT-Head: Network transfer.*?\s+(?:\d|-)+\s+(?<netbyt>\d+)\s+(.*?\[.*?]\s+){2}\[.*?](?:\s+\[(?<netgbph>\d+\.{0,1}\d{0,}) GBPH]){0,1}"
$patternMediaWrite = "Media Write.*?\s+(?:\d|-)+\s+(?<medbyt>\d+)\s+.*?\[.*?](?:\s+\[(?<medgbph>\d+.{0,1}\d{0,}) GBPH]){0,1}"
$patternPhysWrite = "Physical Write.*?\s+(?:\d|-)+\s+(?<physbyt>\d+)\s+.*?\[.*?](?:\s+\[(?<physgbph>\d+.{0,1}\d{0,}) GBPH]){0,1}"

if ($null -eq $logList) {
  $logList = Get-ChildItem "$filePath\*.log" | Select-Object -ExpandProperty FullName
}

if ($newFile) {
  $csv = @("Job ID,Server,Pipe ID,App type,Stream Source,Pipe start,Pipe end,Duration,Signature module,Signature Bytes,Read Pipeline recv,Read Pipeline recv bytes,Network transfer,Network transfer bytes,Media write,Media Write bytes,Physical write,Physical write bytes")
} else {
  $csv = @()
}


foreach ($file in $logList) {
  Write-Progress -Activity "Parsing $file"
  Write-Host "Parsing $file"
  $content = Get-Content $file -Raw
  $sections = $content -split "={87}"
  Write-Host "Found $($sections.Count) pipes to analyze"
#break
  for ($i = 1; $i -le ($sections.count - 1); $i++) {
    Write-Progress -Activity "Parsing $file" -Status "Section $i"
    $src = $sections[$i] -split "`n"

   try {
    # parse values with patterns
    # Job, pipe, app IDs
    $result = $src | Select-String $patternJobPipeApp
    $jobId = $result.Matches.Captures.Groups["jobid"].Value
    $pipeId = $result.Matches.Captures.Groups["pipeid"].Value
    $appType = $result.Matches.Captures.Groups["apptype"].Value

    # Stream source
    $result = $src | Select-String $patternSource
    $source = $result.Matches.Captures.Groups["source"].Value
   
    # Start & end times - convert to standard format
    $result = $src | Select-String $patternStartEnd
    $startTime = Get-Date $result.Matches.Captures.Groups["start"].Value #-Format "MM/dd/yyyy HH:mm:ss"
    $endTime = Get-Date $result.Matches.Captures.Groups["end"].Value #-Format "MM/dd/yyyy HH:mm:ss"
    $duration = $endTime - $startTime

    # Receive data
    $result = $src | Select-String $patternReceive
    if ($null -ne $result) {
      $recvBytes = $result.Matches.Captures.Groups["recvbyt"].Value
      if ($result.Matches.Captures.Groups["recvgbph"].Success) {
        $recvSpeed = $result.Matches.Captures.Groups["recvgbph"].Value
      } else {
        $recvSpeed = 0
      }
    } else {
      $recvBytes = $null
      $recvSpeed = $null
    }

    # Signature module
    $result = $src | Select-String $patternSignature
    if ($null -ne $result) {
      $sigBytes = $result.Matches.Captures.Groups["sigbyt"].Value
      if ($result.Matches.Captures.Groups["siggbph"].Success) {
        $sigSpeed = $result.Matches.Captures.Groups["siggbph"].Value
      } else {
        $sigSpeed = 0
      }
    } else {
      $sigBytes = $null
      $sigSpeed = $null
    }

    # Network transfer
    $result = $src | Select-String $patternNetTransfer
    if ($null -ne $result) {
      $netBytes = $result.Matches.Captures.Groups["netbyt"].Value
      if ($result.Matches.Captures.Groups["netgbph"].Success) {
        $netSpeed = $result.Matches.Captures.Groups["netgbph"].Value
      } else {
        $netSpeed = 0
      }
    } else {
      $netBytes = $null
      $netSpeed = $null
    }

    # Media write
    $result = $src | Select-String $patternMediaWrite
    if ($null -ne $result) {
      $medBytes = $result.Matches.Captures.Groups["medbyt"].Value
      if ($result.Matches.Captures.Groups["medgbph"].Success) {
        $medSpeed = $result.Matches.Captures.Groups["medgbph"].Value
      } else {
        $medSpeed = 0
      }
    } else {
      $medBytes = $null
      $medSpeed = $null
    }

    # Physical write
    $result = $src | Select-String $patternPhysWrite
    if ($null -ne $result) {
      $physBytes = $result.Matches.Captures.Groups["physbyt"]
      if ($result.Matches.Captures.Groups["physgbph"].Success) {
        $physSpeed = $result.Matches.Captures.Groups["physgbph"].Value
      } else {
        $physSpeed = 0
      }
    } else {
      $physBytes = $null
      $physSpeed = $null
    }

    # Build CSV string
    $csv += "$jobId,$server,$pipeId,$appType,$source,$($startTime.ToString('MM/dd/yyyy HH:mm:ss')),$($endTime.ToString('MM/dd/yyyy HH:mm:ss')),$($duration.ToString()),$sigSpeed,$sigBytes,$recvSpeed,$recvBytes,$netSpeed,$netBytes,$medSpeed,$medBytes,$physSpeed,$physBytes"
   }
   catch {
    Write-Host "Errors in section $i"
    Write-Debug $($sections[$i])
   }
  }

}

if ($newFile) {
  $csv | Out-File $outFile
} else {
  $csv | Out-File $outFile -Append
}