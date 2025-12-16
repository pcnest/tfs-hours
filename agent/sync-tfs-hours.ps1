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
SELECT [System.Id]
FROM WorkItems
WHERE
  [System.TeamProject] = @project
  AND [System.WorkItemType] = 'Task'
  AND [System.ChangedDate] >= '$SinceDate'
ORDER BY [System.ChangedDate] ASC
"@
}

$wiqlR = Invoke-Tfs "POST" $WiqlUrl $wiql
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
foreach ($ch in (Chunk $taskIds 200)) {
  $idsCsv = ($ch -join ",")
  $url = "$TfsRoot/$Collection/_apis/wit/workitems?ids=$idsCsv&`$expand=relations&api-version=$ApiV"
  $r = Invoke-Tfs "GET" $url
  if ($r.value) { $tasks += $r.value }
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

foreach ($t in $tasks) {
  $f = $t.fields

  $changed = $f."System.ChangedDate"
  $changedUtc = if ($changed -is [DateTime]) { $changed.ToUniversalTime() } else { [DateTime]::Parse([string]$changed).ToUniversalTime() }
  if ($changedUtc -lt $SinceUtc) { continue }


  $assigned = $f."System.AssignedTo"
  $assignedName = $null
  $assignedUPN = $null
  if ($assigned -is [string]) {
    $assignedName = $assigned
    $assignedUPN = $assigned
  }
  elseif ($assigned) {
    $assignedName = $assigned.displayName
    $assignedUPN = $assigned.uniqueName
  }

  $parentId = Get-ParentIdFromRelations $t.relations
  if ($parentId) { [void]$parentIds.Add($parentId) }

  $taskRows += [pscustomobject]@{
    taskId            = $t.id
    taskTitle         = $f."System.Title"
    taskChangedDate   = $f."System.ChangedDate"
    activity          = $f."Microsoft.VSTS.Common.Activity"
    taskAssignedTo    = $assignedName
    taskAssignedToUPN = $assignedUPN
    actualHours       = $f."SupplyPro.SPApplication.Task.ActualHours"
    parentId          = $parentId
    # parent fields filled later
    parentType        = $null
    parentTitle       = $null
    accountCode       = $null
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
Write-LastSyncUtc $runAtUtc
