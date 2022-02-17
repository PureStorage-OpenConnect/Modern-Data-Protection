<#
.SYNOPSIS
Script to enable Commvault restore functionality from FlashArray File Services snapshots after a data loss event.
.DESCRIPTION
This script will enumerate all of the Commvault chunks in a snapshot, create symbolic links to them within another share, and copy the chunk files into the share in place of the links.
The copy uses robocopy and server-side processing. It can be decoupled and run at a different time.
.EXAMPLE
Execute-CVSafeModeRecovery -SnapshotShare \\myserver\oldshare -LinkShare \\myserver\newshare

Migrates the data from \\myserver\oldshare to \\myserver\newshare. You will be prompted to choose the source snapshot.

.EXAMPLE
Execute-CVSafeModeRecovery -SnapshotShare \\myserver\oldshare -LinkShare \\myserver\newshare -CopyOnly

Copies the data from \\myserver\oldshare to \\myserver\newshare, replacing file and directory links. Terminates if no links exist in \\myserver\newshare.

.NOTES
Prior to running this script, you must enable remote symlink resolution on each MediaAgent system that uses the FlashArray as a disk target. Use "fsutil behavior set SymlinkEvaluation R2R:1". 

Tested on Windows Server 2019. Use on other versions at your own risk.
This script should be tested in a non-production environment before implementing in production.
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
    # UNC path to the share housing the backup data and snapshots
    [Parameter(Mandatory=$true)]
    [ValidateScript({if (Test-Path $_) {$true} else {throw "Path $_ not found"}})]
    [String]$SnapshotShare,
    # UNC path to the share where the data is being restored
    [Parameter(Mandatory=$true)]
    [ValidateScript({if (Test-Path $_) {$true} else {throw "Path $_ not found"}})]
    [String]$LinkShare,
    # Controls whether to bypass creating links, if you have already created them. Fails if links do not exist.
    [switch]$CopyOnly = $false,
    # Full path to use to write the log file. By default, logs will be written to the same location as the script file.
    [String]$LogFile = "$($MyInvocation.InvocationName.Substring(0, $MyInvocation.InvocationName.LastIndexOf("\")))\RecoveryLog-$(Get-Date -Format 'yyyyMMdd-HHmmss').txt",
    # Full path to use to write a summary of operations the script performs. By default, the summary will be written to the same location as the script file.
    [String]$SummaryFile = "$($MyInvocation.InvocationName.Substring(0, $MyInvocation.InvocationName.LastIndexOf("\")))\Summary-$(Get-Date -Format 'yyyyMMdd-HHmmss').txt"
)


function Write-LogFile {
    Param (
        [string]$LogFile,
        [string]$Message,
        [string]$Level = "INFO"
    )
    # Log and echo messages with appropriate severity level

    switch ($level) {
        "WARNING" {
            Write-Warning $Message
        }
        "ERROR" {
            Write-Error $Message
        }
        "INFO" {
            Write-Host $Message
        }
        "DEBUG" {
            Write-Debug $Message
        }
        default {
            Write-Host $Message
        }
    }
    $Message = "$(Get-Date -Format 'MM/dd/yyyy HH:mm:ss')`t$Level`t$Message"
    $Message | Out-File -Append -FilePath $LogFile
}


function Make-SnapshotLinks {
    Param (
        [string]$snapshotPath,
        [string]$LinkShare #,
        #$fileLinks,
        #$dirLinks
    )

    # Hashtable to track links created here
    $linkLists = @{}
    $fileLinks = @()
    $dirLinks = @()
    $failedLinks = @()
    $ignoredLinks = @()

    $start = Get-Date

    Write-Debug "Snapshot path: $snapshotPath"
    # Create links for all files from the snapshot path, up to directory depth 3. This ignores chunk directories.

    $fileList = Get-ChildItem $snapshotPath -File -Depth 3 | Where-Object {$_.FullName.Split("\").Count -le 10}
    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Checking $($fileList.Count) files"
    $progress = 0
    $fLinkCount = 0
    foreach ($file in $fileList) {

        [string]$snapshotFile = $file.FullName

        # translate to link path
        [string]$linkPath = $snapshotFile.Replace($snapshotPath, $LinkShare)
        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Link path: $linkPath"
        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Checking if $linkPath already exists"
        if (! (Test-Path $linkPath)) {
            # Link path does not exist
            Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Path not found" 
            Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Write link $linkPath to $($snapshotFile)"
            try {
                $link = New-Item -ItemType SymbolicLink -Path $linkPath -Value $snapshotFile -Force
                $fileLinks += $link
            }
            catch {
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Failed to create link at $linkPath"
                $failedLinks += "File`tIgnored`t$linkPath"
            }
        } else {
            # Link path exists. Check whether it's a file or link.
            Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Path found"
            $item = Get-Item $linkPath
            Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Item path: $($item.FullNname)"
            #if ((($item.LinkType -is [array]) -and ($null -eq $item.LinkType[0])) -or ($null -eq $item.LinkType)) {
            if ($item.Attributes -notcontains "ReparsePoint") {
                # Item is already a file. Confirm it matches the snapshot.
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "$linkPath is a local file"
                $snapshotFile = Get-Item -Path $item.FullName.Replace($LinkShare, $snapshotPath)
                # Compare modified times and size to snapshot
                if (($item.length -ne $snapshotFile.length) -or ($item.LastWriteTime -ne $snapshotFile.LastWriteTime)) {
                
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Files don't match. Replacing file with link to snapshot."
                    try {
                        Remove-Item -Path $linkPath -Force
                        $link = New-Item -ItemType SymbolicLink -Path $linkPath -Value $snapshotFile -Force
                        $fileLinks += $link
                    }
                    catch {
                        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Failed to replace $linkPath"
                        $failedLinks += "File`tIgnored`t$linkPath"
                    }
                } else {
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Files match. No action."
                    $ignoredLinks += "File`tIgnored`t$($linkPath)"
                }
            } elseif ($item.LinkType -eq "SymbolicLink") {
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Linktype: $($item.LinkType)"
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Linktype type: $($item.LinkType.GetType())"
                # Item is already a link. Confirm it points to the right path.
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "$linkPath is already a link pointing to $($item.Target)"
                # Target property uses "UNC\path" format, replace UNC to get back to actual UNC
                if ($item.Target.Replace("UNC", "\") -ne $snapshotFile) {
                    # Link doesn't point to snapshot already
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Link doesn't match. Deleting and recreating it."
                    try {
                        $item.Delete()
                        $link = New-Item -ItemType SymbolicLink -Path $linkPath -Value $snapshotFile -Force
                        $fileLinks += $link
                    }
                    catch {
                        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Failed to recreate link $linkPath"
                        $failedLinks += "File`tFailed`t$linkPath"
                    }
                } else {
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Link matches. No Action."
                    $ignoredLinks += "File`tIgnored`t$item"
                }
            } else {
                # Unknown object type. Ignore for now.
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "$($item.Name) is an unknown type. Ignoring."
                $ignoredLinks += "File`tIgnored`t$item"
            }
            $progress ++
            Write-Progress -Activity "Creating file links" -CurrentOperation $file.FullName -Status "$progress/$($fileList.Count)" -PercentComplete (($progress / $fileList.Count) * 100)
        }
    }

    # Update hashtable
    $linkLists["file"] = $fileLinks

    # Create links for all directories from snapshot path, at only depth 3
    $dirList = Get-ChildItem $snapshotPath -Dir -Depth 3 | Where-Object {$_.FullName.Split("\").Count -eq 10}
    $progress = 0
    $dLinkCount = 0
    foreach ($dir in $dirList) {
        $snapshotDir = $dir.fullName

        $linkPath = $dir.FullName.Replace($snapshotPath, $LinkShare)
        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Checking if $linkPath already exists"
        if (! (Test-Path $linkPath)) {
            # Link does not exist
            Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Write link $linkPath pointing to $snapshotDir"
            try {
                $link = New-Item -ItemType SymbolicLink -Path $linkPath -Value $snapshotDir -Force
                $dirLinks += $link
                $dLinkCount ++
            }
            catch {
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Failed to create link at $linkPath"
                $failedLinks += "Dir`tFailed`t$linkPath"
            }
        } else {
            # Item exists. Check whether dir or link.
            $item = Get-Item $linkPath
            if ($item.Attributes -notmatch "ReparsePoint") {
#            if ((($item.LinkType -is [array]) -and ($null -eq $item.LinkType[0])) -or ($null -eq $item.LinkType)) {
                # Item is already a dir. Confirm it is matched to snapshot. If any files don't match, replace the directory with a link to the snapshot.
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "$linkPath is a local dir"
                # Compare modified timestamps and sizes of child files to snapshot
                $dirMatch = $true
                foreach ($file in (Get-ChildItem -File -Path $item.FullName)) {
                    $snapshotFile = Get-Item -Path $file.FullName.Replace($LinkShare, $snapshotPath)
                    if (($file.length -ne $snapshotFile.length) -or ($file.LastWriteTime -ne $snapshotFile.LastWriteTime)) {
                        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Files don't match. Going to link to snapshot."
                        $dirMatch = $false
                    } else {
                        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Files match. No action."
                    }
                }
                if (! $dirMatch) {
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Local directory mismatch. Replacing with link to $snapshotPath."
                    try {
                        Remove-Item $item -Recurse -Force
                        $link = New-Item -ItemType SymbolicLink -Path $linkPath -Value $snapshotPath -Force
                        $dirLinks += $link
                    }
                    catch {
                        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Failed to replace directory $($item.FullName)"
                        $failedLinks += "Dir`tFailed`t$($item.FullName)"
                    }
                } else {
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Directory matches. No action."
                    $ignoredLinks += "Dir`tIgnored`t$($item.FullName)"
                }
            } else {
                # Already a link. Confirm it points to the right path.
                Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Already a link at $linkPath pointing to $($item.target). Checking where it points."
                # target property uses "UNC\path" format, replace UNC to get back to actual UNC
                if ($item.Target.Replace("UNC", "\") -ne $snapshotPath) {
                    # link doesn't point to snap already
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Link doesn't match. Will delete and recreate."
                    try {
                        $item.Delete()
                        $link = New-Item -ItemType SymbolicLink -Path $linkPath -value $snapshotPath -Force
                        $dirLinks += $link
                    }
                    catch {
                        Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Failed to replace link $linkPath"
                    }
                } else {
                    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Link matches. No action"
                    $ignoredLinks += "Dir`tIgnored`t$($item.FullName)"
                }
            }
        }
        $progress ++
        Write-Progress -Activity "Creating dir links" -CurrentOperation $dir.FullName -Status "$progress/$($dirList.Count)" -PercentComplete (($progress / $dirList.Count) * 100)
    }

    # Look for existing extras in the share and report, but don't touch them
    #$fileList = Get-ChildItem $snapshotPath -File -Depth 3 | Where-Object {$_.FullName.Split("\").Count -le 10}

    # Update hashtable
    $linkLists["dir"] = $dirLinks
    $linkLists["failed"] = $failedLinks
    $linkLists["ignored"] = $ignoredLinks

    # Output hashtable
    Write-Output $linkLists

    $duration = (Get-Date - $start).TotalSeconds
    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Links completed in $duration seconds"
}



function Copy-SnapshotData {
    Param (
        [string]$snapshotPath,
        [string]$LinkShare,
        $fileLinks,
        $dirLinks
    )

    # Tracking
    $copyList = @{}
    $fileCopy = @()
    $dirCopy = @()
    $ignoreCopy = @()
    $failedCopy = @()

    $start = Get-Date

    # Copy items
    # Files
    # Account for possible data aging and update (need to think thru 2nd part)
    Write-LogFile -LogFile $LogFile -Message "Copying $($fileLinks.Count) files in place of links"
    $filesCopied = 0
    $progress = 0
    foreach ($f in $fileLinks) {
        # Ignore anything Commvault might have aged
        if (Test-Path $f.FullName) {
            $fPath = $f.FullName
            $fSrc = $f.Target.Replace("UNC", "\")
            if (Test-Path $fSrc) {
                try {
                    $f.Delete()
                    Copy-Item $fSrc $fPath -Force
                    $fileCopy += $fSrc
                }
                catch {
                    Write-Logfile -LogFile $LogFile -Level "DEBUG" -Message "Failed to copy $fSrc"
                    $failedCopy += $fSrc
                }
            } else {
                Write-LogFile -LogFile $LogFile -Level "WARNING" -Message "Target not found at $fSrc"
                $ignoreCopy += $fSrc
            }
        }
        $progress ++
        Write-Progress -Activity "Copying files" -CurrentOperation $f.FullName -Status "$progress/$($fileLinks.Count)" -PercentComplete (($progress / $fileLinks.Count) * 100)
    }

    # Child dirs
    Write-LogFile -LogFile $LogFile -Message "Copying $($dirLinks.Count) directories in place of links"
    $dirsCopied = 0
    $progress = 0
    foreach ($d in $dirLinks) {
        # Ignore anything Commvault might have aged already
        if (Test-Path $d.FullName) {
            $d = Get-Item $d.FullName
            $dSrc = ""
            $dPath = $d.FullName
        #    if ($d.Target 
            $dSrc = $d.Target.Replace("UNC", "\")
            $dName = $d.Name
            # Get CPU count to set robocopy threads, capped at 24
            $cpuCount = (Get-WmiObject Win32_Processor | Measure-Object -Property NumberOfCores -Sum).Sum
            $threadCount = [math]::Min($cpuCount * 2, 24)
            # Use robocopy to copy snapshot files into temp directory next to the link. Robocopy will automatically do server-side copy.
            $roboOut = robocopy.exe "$dSrc" "$dPath.tmp" *.* /e /mt:$threadCount /r:1 /w:0
            # some kind of error handling needed
            try {
                $d.Delete()
                # Rename 
                Rename-Item "$dPath.tmp" $dName
                $dirCopy += $dPath
            }
            catch {
                Write-LogFile -LogFile $LogFile -Level "ERROR" -Message "Unable to delete link $dName"
                $failedCopy += $dPath
            }
        }
        $progress ++
        Write-Progress -Activity "Copying dirs" -CurrentOperation $d.FullName -Status "$progress/$($dirLinks.Count)" -PercentComplete (($progress / $dirLinks.Count) * 100)
    }
    $duration = (Get-Date - $start).TotalSeconds
    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Copy completed in $duration seconds"
}


###### Main script ######

$startTime = Get-Date

# Confirm shares exist
if (! (Test-Path $SnapshotShare)) {
    Write-LogFile -LogFile $LogFile -Level "ERROR" -Message "Snapshot share not found at $SnapshotShare"
    exit 2
}

if (! (Test-Path $LinkShare)) {
    Write-LogFile -LogFile $LogFile -Level "ERROR" -Message "Link share not found at $LinkShare"
    exit 2
}

[String]$snapshotPath = "$SnapshotShare\.snapshot\"
#$fileLinks = @()
#$dirLinks = @()
#$linkHash = @{}

# check for -copyonly option
if ($CopyOnly) {
    # Find a link within the share and and get the snapshot path from it to avoid user error
    
    # Search for the first link that points to a snapshot and break out of the pipeline
    $firstLink = Get-ChildItem -Path $LinkShare -Recurse -Attributes ReparsePoint | Where-Object {$_.Target -match ".snapshot"} | Select-Object -First 1
    if ($null -ne $firstLink) {
        # Figure out snapshot name
        $targetSplit = $firstLink.Target.Split('\')
        $snapshotName = $targetSplit[$targetSplit.IndexOf(".snapshot") + 1]
        $recoverySnapshot = Get-Item ("$snapshotShare\.snapshot\$($firstLink.Target.Split('\')[4])")
        if ((Read-Host "Found links to snapshot $recoverySnapshot from $($firstLink.LastWriteTime). Is this correct? [Y/N]") -eq "Y") {
            Write-LogFile -LogFile $LogFile -Message "Copying data"
            $snapshotPath += $recoverySnapshot
            #Get-LinkLists -LinkShare $LinkShare -fileLinks $fileLinks -dirLinks $dirLinks

            # Get links from file share
            $fileLinks = Get-ChildItem -File -Path $LinkShare -Recurse -Attributes ReparsePoint | Where-Object {($_.Target -replace "UNC", "\") -match [RegEx]::Escape($snapshotPath)}
            $dirLinks = Get-ChildItem -Dir -Path $LinkShare -Recurse -Attributes ReparsePoint | Where-Object {($_.Target -replace "UNC", "\") -match [RegEx]::Escape($snapshotPath)}
            # Copy over links
            Copy-SnapshotData -snapshotPath $snapshotPath -LinkShare $LinkShare -fileLinks $fileLinks -dirLinks $dirLinks
        } else {
            Write-LogFile -LogFile $LogFile -Message "Please confirm the desired snapshot and rerun the script."
        }
    } else {
        Write-Host "No links were found in the selected path. If you have not yet created links, please rerun this script without the CopyOnly option."
    }

} else {


    # Do some logic around name lengths to set header spacing
    [object[]]$snapshotList = Get-ChildItem $snapshotPath -Dir | Sort-Object -Property LastWriteTime -Descending
    [int]$nameLength = ($snapshotList | Select-Object -ExpandProperty Name | Measure-Object -Property Length -Maximum).Maximum
    [string]$header = "No.`tSnapshot Name$(' ' * ($nameLength - [math]::min(13, $nameLength)))`tCreation Time (mm/dd/yyyy)"
    [boolean]$snapChosen = $false

    while (! $snapChosen) {
        Clear-Host
        Write-Host "Available Snapshots:"
        Write-Host $header
        for ($i = 1; $i -le $snapshotList.Count; $i++) {
            Write-Host "$i.`t$($snapshotList[$i - 1].Name)$(' ' * ($nameLength - $snapshotList[$i - 1].Name.Length))`t$($snapshotList[$i - 1].LastWriteTime)"
        }
        $snapChoice = (Read-Host "Select snapshot for recovery [1-$($snapshotList.Count)]") - 1
        if ($snapChoice -eq "q") {
            Write-LogFile -LogFile $LogFile -Message "Quitting due to user selection"
            exit 0
        }
        # Confirm choice
        if ((Read-Host "Is $($snapshotList[$snapChoice].Name)`t$($snapshotList[$snapChoice].LastWriteTime) the correct snapshot? [Y/N]") -eq "Y") {
            $snapChosen = $true
        }
    }

    Clear-Host

    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Snap choice: $snapChoice"
    $recoverySnapshot = $snapshotList[$snapChoice]
    Write-LogFile -LogFile $LogFile -Level "DEBUG" -Message "Recovery snapshot: $($recoverySnapshot.Name)"
    $snapshotPath += $recoverySnapshot.Name
    #$snapshotPath = $snapshotPath + (Get-ChildItem $snapshotPath -Dir | Sort-Object -Property LastWriteTime -Descending)[0] # rev to make interactive selection
    
    # Create links
    $linkHash = Make-SnapshotLinks -snapshotPath $snapshotPath -LinkShare $LinkShare -fileLinks $fileLinks -dirLinks $dirLinks
    
    # Write link summary to console
    Write-Host "Summary"
    Write-Host "File links created: $($linkHash["file"].Count)"
    Write-Host "Directory links created: $($linkHash["dir"].Count)"
    Write-Host "Links ignored: $($linkHash["ignored"].Count)"
    Write-Host "Links failed: $($linkHash["failed"].Count)"

    # Summary report
    "Migration Summary" | Out-File -FilePath $SummaryFile
    "Executed at $(Get-Date -Date $startTime -Format "HH:mm:ss on MMM dd, yyy")" | Out-File -FilePath $SummaryFile -Append
    "Operation`tType`tResult`tPath" | Out-File -FilePath $SummaryFile -Append
    foreach ($link in $linkHash["file"]) {
        "Create link`tFile`tSuccess`t$($link.FullName)" | Out-File -FilePath $SummaryFile -Append
    }
    foreach ($link in $linkHash["dir"]) {
        "Create link`tDir`tSuccess`t$($link.FullName)" | Out-File -FilePath $SummaryFile -Append
    }
    # Ignored and failed values include type and result fields
    foreach ($link in $linkHash["ignored"]) {
        "Create link`t$link" | Out-File -FilePath $SummaryFile -Append
    }
    foreach ($link in $linkHash["failed"]) {
        "Create link`t$link" | Out-File -FilePath $SummaryFile -Append
    }


    # prompt for "do you want to start copying the contents?"
    if ((Read-Host "Do you want to start copying the contents now? [Y/N]") -eq "Y") {
        #$fileLinks = Get-ChildItem -File -Path $LinkShare -Recurse -Attributes ReparsePoint | Where-Object {$_.Target -match ".snapshot"}
        #$dirLinks = Get-ChildItem -Dir -Path $LinkShare -Recurse -Attributes ReparsePoint | Where-Object {$_.Target -match ".snapshot"}
        Copy-SnapshotData -snapshotPath $snapshotPath -LinkShare $LinkShare -fileLinks $linkHash["file"] -dirLinks $linkHash["dir"]

    } else {
        Write-Host "You can run the copy later by using the -CopyOnly option"
    }
}

