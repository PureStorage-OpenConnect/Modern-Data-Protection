<#	
	.SYNOPSIS
	A Veeam VBR post-job script to replicate Pure FlashArray snapshots to a secondary FlashArray.
	.DESCRIPTION
	A Veeam VBR post-job script to be used with Pure FlashArray snapshot only job. Will replicate the new snapshot to a secondary FlashArray.
	After replicating, the replica will be cloned to a new volume and then snapped in order to be scannable by the Pure plug-in for Veeam.
	Requires Purity//FA 6.1.0 or later.
	.NOTES
	Acknowledgements: 
	Get Veeam Job Name From Process ID - https://blog.mwpreston.net/2016/11/17/setting-yourself-up-for-success-with-veeam-pre-job-scripts/ 
	.LINK
	https://jdwallace.com
#>

<#===========================================================================
User Configuration - Customize these variables for your environment
===========================================================================#>
# Source FlashArray - Hosting VMFS Datastores being protected - Currently only 1 source FA supported per job
$sourceArrayEndpoint = "fa-1.homelab.local"
# Target FlashArray - Where will the Snapshots be replicated to?
$targetArrayEndpoint = "fa-2.homelab.local"
# User/Pass for FlashArray - Must exist on both FlashArrays. Must be storage-admin or higher
# Required since we're using Invoke-Pfa2CLICommand, will be replaced by OAuth2 in the future
$arrayUsername = "pureuser"
$arrayPassword = "pureuser"
# Name of Target FlashArray Host Group for Veeam Proxies
# Must be pre-created and populated with Veeam Proxy servers in order for Veeam to scan replicas on target FlashArray and inventory VMs
$PfaVeeamHostGroup = "Veeam-Proxies"
# If DisableSourceRetention is TRUE, Snapshots will be removed from Source FlashArray after replication
$DisableSourceRetention = $true # EXPERIMENTAL
# TestMode will use a specific Veeam Job Name, otherwise figure out Job Name based on which one launched the script
$TestMode = $false
$TestModeJobName = "FA Snapshot Job"
# Path to VBR Installation.
$PathToVBRInstall = "C:\Program Files\Veeam\Backup and Replication\Backup"
<#===========================================================================
End - User Configuration
===========================================================================#>

Import-Module PureStoragePowerShellSDK2

# Get VBR Version
$PathToVBRDLL = $PathToVBRInstall + "\Veeam.Backup.Core.dll"
$VBRServer = Get-Item -Path $PathToVBRDLL
$VBRVersion = $VBRServer.VersionInfo.ProductMajorPart

# If running versions prior to v11, add SnapIn
switch ( $VBRVersion ) {
	{$_ -ge 11} {
		Write-Host "Detected VBR v$VBRVersion"
	}
	{$_ -le 10} {
		Write-Host "Detected VBR v$VBRVersion"
		Add-PSSnapin VeeamPSSnapIn
	}
	default {
		Write-Host "No VBR Installation detected at $PathToVBRInstall"
		Exit 1
	}
}

# Primary FlashArray Credential for user/password authentication
$arrayPasswordSS = ConvertTo-SecureString $arrayPassword -AsPlainText -Force
$arrayCredential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $arrayUsername, $arrayPasswordSS 

# Connect to FlashArrays with user/password
$sourceFAConnection = Connect-Pfa2Array -EndPoint $sourceArrayEndpoint -Credential $arrayCredential -IgnoreCertificateError
$targetFAConnection = Connect-Pfa2Array -EndPoint $targetArrayEndpoint -Credential $arrayCredential -IgnoreCertificateError
$sourceFA = Get-Pfa2Array -Array $sourceFAConnection
$targetFA = Get-Pfa2Array -Array $targetFAConnection
switch ( $sourceFA.Version) {
	{$_ -ge 6.1} {
		Write-Host "Source Purity//FA Version: $($sourceFA.Version)"
	}
	default {
		Write-Host "Unsupported Purity//FA Version. Minimum version: 6.1 "
		Write-Host "Target Purity//FA Detected Version: $($sourceFA.Version)"
		Exit 1
	}
}
switch ( $targetFA.Version) {
	{$_ -ge 6.1} {
		Write-Host "Target Purity//FA Version: $($targetFA.Version)"
	}
	default {
		Write-Host "Unsupported Purity//FA Version. Minimum version: 6.1 "
		Write-Host "Target Purity//FA Detected Version: $($targetFA.Version)"
		Exit 1
	}
}

if ($TestMode) {
	# Get specific job
	$VbrJob = Get-VBRJob | Where-Object { $_.Name -eq $TestModeJobName }
	Write-Host "Executing in Test Mode with VBR Job $TestModeJobName"
}else {
	# Get Veeam job name from parent process id
	$parentpid = (Get-WmiObject Win32_Process -Filter "processid='$pid'").parentprocessid.ToString()
	$parentcmd = (Get-WmiObject Win32_Process -Filter "processid='$parentpid'").CommandLine
	$jobid = $parentcmd.split('" "')[16]
	$VbrJob = get-vbrjob | where-object { $_.Id -eq "$jobid" }
	Write-Host "Executing with VBR Job: $($VbrJob.Name)"
}

# Get latest job Session (should be the session that called this script)
switch ( $VBRVersion ) {
	{$_ -ge 11} {
		$VbrSession = Get-VBRBackupSession | Where-Object {$_.jobId -eq $VbrJob.Id} | Sort-Object CreationTimeUTC -Descending | Select-Object -First 1
	}
	{$_ -le 10} {
		$VbrSession = Get-VBRBackupSession | Where-Object {$_.jobId -eq $VbrJob.Id.Guid} | Sort-Object CreationTimeUTC -Descending | Select-Object -First 1
	}
}

# Get all FlashArray snapshots created from this Job
$jobNameInSnap = ".VEEAM-ProdSnap-" + $VbrJob.Name -replace ' ','-'
$PfaSnapshots = Get-StoragePluginSnapshot | Where-Object { $_.Name -cmatch $jobNameInSnap }
if ($PfaSnapshots.Length -eq 0) { Exit 0 }
$PfaConfirmedSnapshots = New-Object -TypeName System.Collections.ArrayList

#For each FlashArray snapshot, check that it's from the latest session and replicate if so
ForEach ($PfaSnapshot in $PfaSnapshots) {
	# If Snapshot was created after latest session start time, it must have been created during this session
	If ($PfaSnapshot.CreationTimeSrv -gt $VbrSession.Info.CreationTime) {
		# Ensure snapshot still exists
		# TODO See if there is a way to force a StoragePluginVolume Rescan and wait for completion instead
		$PfaSnapshotTest = Get-Pfa2VolumeSnapshot -Array $sourceFAConnection -Name $PfaSnapshot.Name
		if ($PfaSnapshotTest.Name -eq $PfaSnapshot.Name) {
			# Construct 'purevol send' command
			$snapSendCLI = "purevol send --to " + $targetFA.Name + " " + $PfaSnapshot.Name
			# Invoke-Pfa2CLICommand to call 'purevol send' to replicate the snapshot
			Invoke-Pfa2CLICommand -EndPoint $sourceArrayEndpoint -Credential $arrayCredential -CommandText $snapSendCLI
			$PfaConfirmedSnapshots.Add( $PfaSnapshot )
		}
	}
}

# For each FlashArray snapshot which was replicated, clone, snap, then clean up
ForEach ($PfaConfirmedSnapshot in $PfaConfirmedSnapshots) {
	# Wait for replication to complete
	$seconds = 0
	do {
		Start-Sleep -Seconds $seconds
		$replicationStatus = Get-Pfa2VolumeSnapshotTransfer -Array $sourceFAConnection -Name $PfaConfirmedSnapshot.Name
		$seconds = 1
	} while ($null -eq $replicationStatus.Completed)
	
	# Get Source and Replica Snapshots
	$PfaSnapshotSource = Get-Pfa2VolumeSnapshot -Array $sourceFAConnection -Name $PfaConfirmedSnapshot.Name
	$PfaSnapshotReplica = Get-Pfa2VolumeSnapshot -Array $targetFAConnection -Name ($sourceFA.Name + ":" + $PfaConfirmedSnapshot.Name)

	# Clone Replicated Snapshot, Overwriting existing
	$src = New-Pfa2ReferenceObject -Id $PfaSnapshotReplica.id -Name $PfaSnapshotReplica.Name
	$PfaSnapshotClone = New-Pfa2Volume -Array $targetFAConnection -Overwrite $true -Source $src -Name ($PfaSnapshotReplica.Source.Name.Split(':')[1] + "-VEEAM-ProdSnap-Replica")
	# Connect clone to Veeam Proxy Host Group if it isn't connected
	if ($null -eq (Get-Pfa2Connection -Array $targetFAConnection -HostGroupNames $PfaVeeamHostGroup -VolumeNames $PfaSnapshotClone.Name)) {
		New-Pfa2Connection -Array $targetFAConnection -HostGroupNames $PfaVeeamHostGroup -VolumeNames $PfaSnapshotClone.Name
	}
	
	# Create Snapshot of Clone
	$PfaCloneSnapshot = New-Pfa2VolumeSnapshot -Array $targetFAConnection -SourceNames $PfaSnapshotClone.Name
	# Wait until Snapshot is created
	while ($null -eq $PfaCloneSnapshot.Created) {
		Start-Sleep -Seconds 1
		$PfaCloneSnapshot = Get-Pfa2VolumeSnapshot -Array $targetFAConnection -IDs $PfaCloneSnapshot.Id
	}

	#Get all of clone's snapshots 
	$PfaReplicaCloneSnaps = Get-Pfa2VolumeSnapshot -Array $targetFAConnection -Destroyed $false -SourceNames $PfaSnapshotClone.Name | Sort-Object Created -Descending 

	# See if number of snaps exceeds Job retention policy
	if ($PfaReplicaCloneSnaps.Length -gt $VbrJob.Options.BackupStorageOptions.RetainCycles) {
		#Delete the excess snapshots
		For ($i = $VbrJob.Options.BackupStorageOptions.RetainCycles; $i -lt $PfaReplicaCloneSnaps.Length; $i++ ) {
			Remove-Pfa2VolumeSnapshot -Array $targetFAConnection -Name $PfaReplicaCloneSnaps[$i].Name
		}
	}
	
	# Delete old Snapshot Replicas from Target FlashArray but keep latest as baseline
	$PfaTempSnapshots = Get-Pfa2VolumeSnapshot -Array $targetFAConnection -Destroyed $false | Where-Object { $_.Name -cmatch ($PfaSnapshotReplica.Name.Split('.')[0] + ".VEEAM-ProdSnap-FA-Snapshot-Job") }
	ForEach ($PfaTempSnapshot in $PfaTempSnapshots) {
		If ($PfaTempSnapshot.Id -ne $PfaSnapshotReplica.Id) { 
			Remove-Pfa2VolumeSnapshot -Array $targetFAConnection -Ids $PfaTempSnapshot.Id
		}
	}

	# Delete old Snapshots from Source FlashArray but keep latest as baseline
	if ($DisableSourceRetention) {
		<# TODO Need to wait until the new source snapshot has become the baseline. Not sure how to test for this.
		   For now just wait some amount of time and try. Even if it's not the baseline yet you'll just end
		   up with an extra snap which will get cleaned up next time the job runs. #>
		Start-Sleep -Seconds (30)
		$PfaTempSnapshots = Get-Pfa2VolumeSnapshot -Array $sourceFAConnection -Destroyed $false | Where-Object { $_.Name -cmatch ($PfaSnapshotSource.Name.Split('.')[0] + ".VEEAM-ProdSnap-FA-Snapshot-Job") }
		ForEach ($PfaTempSnapshot in $PfaTempSnapshots) {
			If ($PfaTempSnapshot.Id -ne $PfaSnapshotSource.Id) {
				Remove-Pfa2VolumeSnapshot -Array $sourceFAConnection -Ids $PfaTempSnapshot.Id
			}
		}
	}
}
# Disconnect from FlashArray
Disconnect-Pfa2Array -Array $sourceFAConnection
Disconnect-Pfa2Array -Array $targetFAConnection

# Tell VBR to Rescan Connected FlashArrays
$PlugInHosts = Get-StoragePluginHost -PluginType "FlashArray"
ForEach ($PlugInHost in $PlugInHosts) {
	Sync-StoragePluginHost -Host $PlugInHost
	$PlugInVolumes = Get-StoragePluginVolume -Host $PlugInHost.Name
	ForEach ($PlugInVolume in $PlugInVolumes) {
		Sync-StoragePluginVolume -Volume $PlugInVolume
	}
}
