<#
.SYNOPSIS
Consolidate Commvault performance statistics.

.DESCRIPTION
This script will parse all of the data pipe statistics from one or more Commvault performance analysis logs and consolidate key stats into a single CSV file. Works with CvPerfMgr.log collected from Windows, Linux and UNIX systems.
Current stats are:
Pipe start, end and duration times
Time spent (if available) and bytes processed for signature module, read pipeline receive, network transfer, media write and physical write.

.EXAMPLE
Convert-CVPerfMgr -server myserver -filePath "C:\Temp\PerfLogs" -outFile "C:\Temp\MyPerfData.csv" -newFile $true

Parses pipe statistics from all CvPerfMgr*.log files found in the C:\Temp\PerfLogs directory, attributing them to myserver. Output is written to C:\Temp\MyPerfData.csv, overwriting any existing file. 

.NOTES
Disclaimer
    The sample module and documentation are provided AS IS and are not supported by
	the author or the author's employer, unless otherwise agreed in writing. You bear
	all risk relating to the use or performance of the sample script and documentation.
	The author and the author’s employer disclaim all express or implied warranties
	(including, without limitation, any warranties of merchantability, title, infringement
	or fitness for a particular purpose). In no event shall the author, the author's employer
	or anyone else involved in the creation, production, or delivery of the scripts be liable
	for any damages whatsoever arising out of the use or performance of the sample script and
	documentation (including, without limitation, damages for loss of business profits,
	business interruption, loss of business information, or other pecuniary loss), even if
	such person has been advised of the possibility of such damages.
#>

Param (
    # Name of the server that created the logs.
    [Parameter(Mandatory=$true)]
    $server = "",
    # Path to the directory where the log files are stored.
    $filePath = ".",
    # Array of log files to process. By default, all unzipped logs will be processed.
    $logList = $null,
    # Path to the CSV file to write.
    $outFile = ".\perfdata.csv",
    # Whether to create a new output file. If passed as $true, any existing file will be overwritten.
    $newFile = $false
)

# Regex patterns with named captures
$patternJobPipeApp = "Job-ID: (?<jobid>\d+)\s+\[Pipe-ID: (?<pipeid>\d+)]\s+\[App-Type: (?<apptype>\d+)]"
$patternSource = "Stream Source:\s+(?<source>.+)"
$patternStartEnd = "Head duration \(Local\):\s+\[(?<start>\d+,\w+,\d+ \d{2}:\d{2}:\d{2})\s+~\s+(?<end>\d+,\w+,\d+ \d{2}:\d{2}:\d{2})"
$patternReceive = "SDT: Receive Data.*?\s+(?:\d|-)+\s+(?<recvbyt>\d+)\s+(.*?\[.*?]\s+){2}\[.*?](?:\s+\[(?<recvgbph>\d+\.{0,1}\d{0,}) GBPH]){0,1}"
$patternSignature = "SDT-Head: Signature module.*\s+(?:\d|-)+\s+(?<sigbyt>\d+)\s+(.*?\[.*?]\s+){2}\[.*?](?:\s+\[(?<siggbph>\d+\.{0,1}\d{0,}) GBPH]){0,1}"
$patternNetTransfer = "SDT-Head: Network transfer.*?\s+(?:\d|-)+\s+(?<netbyt>\d+)\s+(.*?\[.*?]\s+){2}\[.*?](?:\s+\[(?<netgbph>\d+\.{0,1}\d{0,}) GBPH]){0,1}"
$patternMediaWrite = "Media Write.*?\s+(?:\d|-)+\s+(?<medbyt>\d+)\s+.*?\[.*?](?:\s+\[(?<medgbph>\d+.{0,1}\d{0,}) GBPH]){0,1}"
$patternPhysWrite = "Physical Write.*?\s+(?:\d|-)+\s+(?<physbyt>\d+)\s+.*?\[.*?](?:\s+\[(?<physgbph>\d+.{0,1}\d{0,}) GBPH]){0,1}"

# Get log list if not specified
if ($null -eq $logList) {
  $logList = Get-ChildItem "$filePath\*.log" | Select-Object -ExpandProperty FullName
}

if ($newFile) {
  # Add CSV header if creating a new file
  $csv = @("Job ID,Server,Pipe ID,App type,Stream Source,Pipe start,Pipe end,Duration,Signature module,Signature Bytes,Read Pipeline recv,Read Pipeline recv bytes,Network transfer,Network transfer bytes,Media write,Media Write bytes,Physical write,Physical write bytes")
} else {
  $csv = @()
}

# Parse each log file in $logList
foreach ($file in $logList) {
  Write-Progress -Activity "Parsing $file"
  Write-Host "Parsing $file"

  # Read text in raw format to prevent automatic line breaks
  $content = Get-Content $file -Raw
  # Split into sections on string of 87 "="
  $sections = $content -split "={87}"
  Write-Host "Found $($sections.Count) pipes to analyze"

  # Process by file sections
  for ($i = 1; $i -le ($sections.count - 1); $i++) {
    Write-Progress -Activity "Parsing $file" -Status "Section $i"
    $src = $sections[$i] -split "`n"

   try {
    # Parse values using appropriate regex patterns and named capture groups
    # Job, pipe, app IDs
    $result = $src | Select-String $patternJobPipeApp
    $jobId = $result.Matches.Captures.Groups["jobid"].Value
    $pipeId = $result.Matches.Captures.Groups["pipeid"].Value
    $appType = $result.Matches.Captures.Groups["apptype"].Value

    # Stream source
    $result = $src | Select-String $patternSource
    $source = $result.Matches.Captures.Groups["source"].Value
   
    # Start & end times
    $result = $src | Select-String $patternStartEnd
    $startTime = Get-Date $result.Matches.Captures.Groups["start"].Value
    $endTime = Get-Date $result.Matches.Captures.Groups["end"].Value
    $duration = $endTime - $startTime

    # Receive data
    $result = $src | Select-String $patternReceive
    if ($null -ne $result) {
      $recvBytes = $result.Matches.Captures.Groups["recvbyt"].Value
      # Stats may not include throughput if duration or sample count is too low
      if ($result.Matches.Captures.Groups["recvgbph"].Success) {
        $recvSpeed = $result.Matches.Captures.Groups["recvgbph"].Value
      } else {
        $recvSpeed = 0
      }
    } else {
      # Some app types may not include this stat
      $recvBytes = $null
      $recvSpeed = $null
    }

    # Signature module
    $result = $src | Select-String $patternSignature
    if ($null -ne $result) {
      $sigBytes = $result.Matches.Captures.Groups["sigbyt"].Value
      # Stats may not include throughput if duration or sample count is too low
      if ($result.Matches.Captures.Groups["siggbph"].Success) {
        $sigSpeed = $result.Matches.Captures.Groups["siggbph"].Value
      } else {
        $sigSpeed = 0
      }
    } else {
      # Some app types, and non-dedupe jobs, may not include this stat
      $sigBytes = $null
      $sigSpeed = $null
    }

    # Network transfer
    $result = $src | Select-String $patternNetTransfer
    if ($null -ne $result) {
      $netBytes = $result.Matches.Captures.Groups["netbyt"].Value
      # Stats may not include throughput if duration or sample count is too low
      if ($result.Matches.Captures.Groups["netgbph"].Success) {
        $netSpeed = $result.Matches.Captures.Groups["netgbph"].Value
      } else {
        $netSpeed = 0
      }
    } else {
      # Some app types may not include this stat
      $netBytes = $null
      $netSpeed = $null
    }

    # Media write
    $result = $src | Select-String $patternMediaWrite
    if ($null -ne $result) {
      $medBytes = $result.Matches.Captures.Groups["medbyt"].Value
      # Stats may not include throughput if duration or sample count is too low
      if ($result.Matches.Captures.Groups["medgbph"].Success) {
        $medSpeed = $result.Matches.Captures.Groups["medgbph"].Value
      } else {
        $medSpeed = 0
      }
    } else {
      # Some app types may not include this stat
      $medBytes = $null
      $medSpeed = $null
    }

    # Physical write
    $result = $src | Select-String $patternPhysWrite
    if ($null -ne $result) {
      $physBytes = $result.Matches.Captures.Groups["physbyt"]
      # Stats may not include throughput if duration or sample count is too low
      if ($result.Matches.Captures.Groups["physgbph"].Success) {
        $physSpeed = $result.Matches.Captures.Groups["physgbph"].Value
      } else {
        $physSpeed = 0
      }
    } else {
      # Some app types may not include this stat
      $physBytes = $null
      $physSpeed = $null
    }

    # Build CSV string and add to CSV array. Format times as MM/dd/yyyy HH:mm:ss.
    $csv += "$jobId,$server,$pipeId,$appType,$source,$($startTime.ToString('MM/dd/yyyy HH:mm:ss')),$($endTime.ToString('MM/dd/yyyy HH:mm:ss')),$($duration.ToString()),$sigSpeed,$sigBytes,$recvSpeed,$recvBytes,$netSpeed,$netBytes,$medSpeed,$medBytes,$physSpeed,$physBytes"
   }
   catch {
    Write-Host "Errors in section $i"
    Write-Debug $($sections[$i])
   }
  }

}

# Write CSV to file
if ($newFile) {
  $csv | Out-File $outFile
} else {
  $csv | Out-File $outFile -Append
}

Write-Host "Processing complete"