Snapshot: scripts_snapshot_20260207_172508.zip
Location: C:\Quant\snapshots
Restore steps:
1. Backup current scripts: Copy-Item -Path C:\Quant\scripts -Destination C:\Quant\scripts_backup_YYYYMMDD_HHMMSS -Recurse -Force
2. Expand-Archive -Path "<snapshot.zip>" -DestinationPath C:\Quant\scripts -Force
