<#
sync-tfs-hours.ps1
- Pulls Tasks changed since last sync
- Reads:
  Task: System.Id, System.Title, System.AssignedTo, System.ChangedDate, Microsoft.VSTS.Common.Activity, SupplyPro.SPApplication.Task.ActualHours
  Parent: System.Id, System.WorkItemType, System.Title, SupplyPro.SPApplication.Bug.ProjectTypeCode (parent-only)
- Joins Task -> Parent via Hierarchy-Reverse link
- POSTs to Render /api/tfs-hours-sync

Env vars required:
  TFS_HOST        e.g. https://remote.spdev.us
  TFS_COLLECTION  e.g. SupplyPro.Applications
  TFS_PROJECT     e.g. SupplyPro.Core
  TFS_PAT         your PAT
  SYNC_URL        e.g. https://your-render-app.onrender.com/api/tfs-hours-sync
  SYNC_API_KEY    same as Render env
Optional:
  API_VERSION     default 2.0
  SINCE_DAYS      default 30 (only used if last_sync.json missing)
#>

$ErrorActionPreference = "Stop"

$HostUrl = $env:TFS_HOST
$Collection = $env:TFS_COLLECTION
$Project = $env:TFS_PROJECT
$Pat = $env:TFS_PAT
$SyncUrl = $env:SYNC_URL
$SyncKey = $env:SYNC_API_KEY
$ApiV = if ($env:API_VERSION) { $env:API_VERSION } else { "2.0" }
$SinceDays = if ($env:SINCE_DAYS) { [int]$env:SINCE_DAYS } else { 30 }

# Normalize base: allow TFS_HOST to be either https://remote.spdev.us or https://remote.spdev.us/tfs
$TfsRoot = ($HostUrl ?? "").TrimEnd("/")
if ($TfsRoot -notmatch "/tfs$") { $TfsRoot = "$TfsRoot/tfs" }


if (-not $HostUrl -or -not $Collection -or -not $Project -or -not $Pat -or -not $SyncUrl) {
  throw "Missing env vars. Need TFS_HOST, TFS_COLLECTION, TFS_PROJECT, TFS_PAT, SYNC_URL."
}

$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$StatePath = Join-Path $Here "last_sync.json"

function Get-BasicAuthHeader($pat) {
  $bytes = [System.Text.Encoding]::UTF8.GetBytes(":$pat")
  $b64 = [Convert]::ToBase64String($bytes)
  return @{ Authorization = "Basic $b64" }
}

$Headers = Get-BasicAuthHeader $Pat

function Invoke-Tfs($method, $url, $body = $null) {
  if ($null -ne $body) {
    $json = ($body | ConvertTo-Json -Depth 50)
    return Invoke-RestMethod -Method $method -Uri $url -Headers $Headers -ContentType "application/json" -Body $json
  }
  else {
    return Invoke-RestMethod -Method $method -Uri $url -Headers $Headers
  }
}

function Get-WorkItemUpdatesAll([int]$id) {
  $all = @()
  $top = 200
  $skip = 0

  while ($true) {
    $url = "$TfsRoot/$Collection/_apis/wit/workitems/$id/updates?`$top=$top&`$skip=$skip&api-version=$ApiV"
    $r = Invoke-Tfs "GET" $url
    if (-not $r -or -not $r.value -or $r.value.Count -eq 0) { break }

    # Only keep updates that actually change ActualHours
    $relevantUpdates = $r.value | Where-Object { $_.fields."SupplyPro.SPApplication.Task.ActualHours" }
    $all += $relevantUpdates

    if ($r.value.Count -lt $top) { break }
    $skip += $top
  }

  return $all
}



function Chunk($arr, $size) {
  $out = @()
  for ($i = 0; $i -lt $arr.Count; $i += $size) {
    $out += , ($arr[$i..([Math]::Min($i + $size - 1, $arr.Count - 1))])
  }
  return $out
}

function Read-LastSyncUtc() {
  if (Test-Path $StatePath) {
    try {
      $j = Get-Content $StatePath -Raw | ConvertFrom-Json
      if ($j -and $j.lastSyncUtc) { return [DateTime]::Parse($j.lastSyncUtc).ToUniversalTime() }
    }
    catch { }
  }
  return ([DateTime]::UtcNow.AddDays(-$SinceDays))
}

function Write-LastSyncUtc([DateTime]$dtUtc) {
  $obj = @{ lastSyncUtc = $dtUtc.ToString("o") }
  $obj | ConvertTo-Json | Set-Content -Path $StatePath -Encoding UTF8
}

function Get-IdentityDisplayNameFromString([string]$s) {
  if ([string]::IsNullOrWhiteSpace($s)) { return $null }
  $m = [regex]::Match($s, "^(.*?)\s*<.+?>\s*$")
  if ($m.Success) { return $m.Groups[1].Value.Trim() }
  return $s.Trim()
}

function Get-IdentityUpnFromString([string]$s) {
  if ([string]::IsNullOrWhiteSpace($s)) { return $null }
  $m = [regex]::Match($s, "<(.+?)>")
  if ($m.Success) { return $m.Groups[1].Value.Trim() }
  return $s.Trim()
}



# --- Since watermark ---
$SinceUtc = Read-LastSyncUtc
$SinceIso = $SinceUtc.ToString("o")            # keep full precision for post-filter + logging
$SinceDate = $SinceUtc.ToString("yyyy-MM-dd")   # WIQL-safe (date only)

Write-Host "Since UTC (watermark): $SinceIso"
Write-Host "WIQL date floor:       $SinceDate"


# --- WIQL: Tasks changed since ---
$WiqlUrl = "$TfsRoot/$Collection/$Project/_apis/wit/wiql?api-version=$ApiV"


$wiql = @{
  query = @"
SELECT [System.Id], [System.ChangedDate]
FROM WorkItems
WHERE
  [System.TeamProject] = @project
  AND [System.WorkItemType] = 'Task'
  AND [System.ChangedDate] >= '$SinceDate'
ORDER BY [System.ChangedDate] ASC
"@
}

$start = Get-Date
$wiqlR = Invoke-Tfs "POST" $WiqlUrl $wiql
Write-Host "WIQL query took: $((Get-Date) - $start).TotalSeconds seconds."

$taskIds = @()
if ($wiqlR.workItems) {
  $taskIds = $wiqlR.workItems | ForEach-Object { $_.id }
}

if (-not $taskIds -or $taskIds.Count -eq 0) {
  Write-Host "No tasks changed since $SinceIso"
  # still advance watermark to now so you don't re-scan repeatedly
  Write-LastSyncUtc ([DateTime]::UtcNow)
  exit 0
}

Write-Host "Found task ids: $($taskIds.Count)"

# --- Fetch task details (include relations for parent) ---


$tasks = @()
$jobs = @()
foreach ($ch in (Chunk $taskIds 100)) {
  # Adjust chunk size to 100 for parallel fetching
  $idsCsv = ($ch -join ",")
  $url = "$TfsRoot/$Collection/_apis/wit/workitems?ids=$idsCsv&`$expand=relations&api-version=$ApiV"

  # Start each API fetch in parallel using Start-Job
  $jobs += Start-Job -ScriptBlock {
    $url = $using:url  # Use $using: to pass the $url variable to the job
    $result = Invoke-Tfs "GET" $url
    return $result.value
  }
}

# Collect results from all jobs after they complete
$jobs | ForEach-Object {
  $job = $_
  $result = Receive-Job -Job $job
  if ($result) {
    $tasks += $result
  }
  Remove-Job -Job $job  # Clean up the job after receiving the result
}


# --- Extract parent ids from Hierarchy-Reverse link ---
function Get-ParentIdFromRelations($rels) {
  if (-not $rels) { return $null }
  $rel = $rels | Where-Object { $_.rel -eq "System.LinkTypes.Hierarchy-Reverse" } | Select-Object -First 1
  if (-not $rel -or -not $rel.url) { return $null }
  # URL ends with .../workItems/{id}
  $m = [regex]::Match($rel.url, "/workItems/(\d+)$")
  if ($m.Success) { return [int]$m.Groups[1].Value }
  return $null
}

$parentIds = New-Object System.Collections.Generic.HashSet[int]
$taskRows = @()

$MaxSeenUtc = $SinceUtc

foreach ($t in $tasks) {
  $f = $t.fields

  $changed = $f."System.ChangedDate"
  $changedUtc = if ($changed -is [DateTime]) { $changed.ToUniversalTime() } else { [DateTime]::Parse([string]$changed).ToUniversalTime() }
  if ($changedUtc -lt $SinceUtc) { continue }

  if ($changedUtc -gt $MaxSeenUtc) { $MaxSeenUtc = $changedUtc }

  $assigned = $f."System.AssignedTo"
  $assignedName = $null
  $assignedUPN = $null
  if ($assigned -is [string]) {
    $assignedName = Get-IdentityDisplayNameFromString $assigned
    $assignedUPN = Get-IdentityUpnFromString $assigned
  }
  elseif ($assigned) {
    $assignedName = $assigned.displayName
    $assignedUPN = $assigned.uniqueName
  }

  $parentId = Get-ParentIdFromRelations $t.relations
  if ($parentId) { [void]$parentIds.Add($parentId) }

  # Pull update history and emit entries only when ActualHours changed
  $updates = Get-WorkItemUpdatesAll ([int]$t.id)

  $events = @()
  foreach ($u in $updates) {
    # revisedDate is the timestamp of that revision/update
    $revDateRaw = $u.revisedDate
    if ([string]::IsNullOrWhiteSpace([string]$revDateRaw)) { continue }

    $revUtc = ([DateTime]::Parse([string]$revDateRaw)).ToUniversalTime()
    if ($revUtc -lt $SinceUtc) { continue }

    # Only keep updates where ActualHours changed
    $hField = $u.fields."SupplyPro.SPApplication.Task.ActualHours"
    if (-not $hField) { continue }

    $newH = $hField.newValue
    $events += [pscustomobject]@{
      changedUtc = $revUtc
      actualH    = $newH
      revId      = ($u.rev ?? $u.id)  # best-effort (varies by server)
      activity   = ($u.fields."Microsoft.VSTS.Common.Activity"?.newValue)
      assigned   = ($u.fields."System.AssignedTo"?.newValue)
      title      = ($u.fields."System.Title"?.newValue)
    }

    if ($revUtc -gt $MaxSeenUtc) { $MaxSeenUtc = $revUtc }
  }

  # If you want to still emit 1 row when there were no ActualHours changes (optional), keep this OFF for now:
  if ($events.Count -eq 0) { continue }

  foreach ($ev in ($events | Sort-Object changedUtc, revId)) {
    $evAssignedName = $assignedName
    $evAssignedUpn = $assignedUPN

    if ($ev.assigned) {
      $evAssignedName = Get-IdentityDisplayNameFromString ([string]$ev.assigned)
      $evAssignedUpn = Get-IdentityUpnFromString ([string]$ev.assigned)
    }

    $taskRows += [pscustomobject]@{
      taskId            = $t.id
      taskTitle         = ($ev.title ?? $f."System.Title")
      taskChangedDate   = $ev.changedUtc.ToString("o")
      activity          = ($ev.activity ?? $f."Microsoft.VSTS.Common.Activity")
      taskAssignedTo    = $evAssignedName
      taskAssignedToUPN = $evAssignedUpn
      actualHours       = $ev.actualH
      parentId          = $parentId
      parentType        = $null
      parentTitle       = $null
      accountCode       = $null
    }
  }

}

Write-Host "Unique parent ids: $($parentIds.Count)"

# --- Fetch parent details ---
$parentsMap = @{} # id -> parent object
if ($parentIds.Count -gt 0) {
  $parentFields = @(
    "System.Id",
    "System.WorkItemType",
    "System.Title",
    "SupplyPro.SPApplication.Bug.ProjectTypeCode"
  ) -join ","

  $pidArr = New-Object 'int[]' $parentIds.Count
  $parentIds.CopyTo($pidArr)

  foreach ($ch in (Chunk $pidArr 200)) {
    $idsCsv = ($ch -join ",")
    $url = "$TfsRoot/$Collection/_apis/wit/workitems?ids=$idsCsv&fields=$parentFields&api-version=$ApiV"
    $r = Invoke-Tfs "GET" $url
    if ($r.value) {
      foreach ($p in $r.value) {
        $parentsMap[[string]$p.id] = $p
      }
    }
  }
}

# --- Join parent info into rows ---
foreach ($row in $taskRows) {
  if ($row.parentId -and $parentsMap.ContainsKey([string]$row.parentId)) {
    $p = $parentsMap[[string]$row.parentId]
    $pf = $p.fields
    $row.parentType = $pf."System.WorkItemType"
    $row.parentTitle = $pf."System.Title"
    $row.accountCode = $pf."SupplyPro.SPApplication.Bug.ProjectTypeCode"
  }
}

# --- POST to Render ---
$runAtUtc = [DateTime]::UtcNow
$payload = @{
  source      = "tfs-hours-sync"
  syncedAtUtc = $runAtUtc.ToString("o")
  rows        = $taskRows
}

$syncHeaders = @{
  "Content-Type" = "application/json"
}
if ($SyncKey) { $syncHeaders["x-api-key"] = $SyncKey }

Write-Host "Posting rows: $($taskRows.Count) -> $SyncUrl"

$bodyJson = ($payload | ConvertTo-Json -Depth 50)
$r = Invoke-RestMethod -Method POST -Uri $SyncUrl -Headers $syncHeaders -Body $bodyJson

Write-Host "SYNC OK: runId=$($r.runId) runAt=$($r.runAt) count=$($r.count)"

# advance watermark
Write-Host "Advancing watermark to (max seen): $($MaxSeenUtc.ToString('o'))"
Write-LastSyncUtc $MaxSeenUtc

