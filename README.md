# Modern Data Protection Sample Scripts

**These scripts are provided as examples and are freely available for modifications and use.**

**Scripts may have dependencies on external modules and tools, such as [VMware PowerCLI](https://developer.vmware.com/web/tool/12.5.0/vmware-powercli) or [fsutil](https://docs.microsoft.com/en-us/windows-server/administration/windows-commands/fsutil). See scripts for specific requirements.**
## Scripts
Updated Feb 22, 2022
### Commvault Backup & Recovery
 - [Execute-CVSafeModeRecovery.ps1](../main/Execute-CVSafeModeRecovery.ps1) -- Recovers a Commvault library mount path on FlashArray File Services into a new managed directory, using a snapshot of the original managed directory. Intended for use after a significant event damages the data in the Commvault library. Requires [fsutil](https://docs.microsoft.com/en-us/windows-server/administration/windows-commands/fsutil) and [robocopy](https://docs.microsoft.com/en-us/windows-server/administration/windows-commands/robocopy).
 - [Convert-CvPerfMgr.ps1](../main/Convert-CvPerfMgr.ps1) -- Parses key data pipe statistics from one or more CvPerfMgr.log files and consolidates them into a single CSV file.

### Veeam Backup & Replication
- [Invoke-PfaSendSnapAfterVBRJob.ps1](../main/Invoke-PfsSendSnapAfterVBRJob.ps1) -- Replicates a snap-only job on Pure FlashArray, executed as a VBR post-backup script, to a secondary FlashArray. Clones the replica to a new volume and then snaps it so it will be usable by the Pure plug-in for Veeam. Requires Purity//FA 6.1.0 or later.
