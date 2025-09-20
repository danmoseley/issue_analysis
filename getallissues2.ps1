param(
  [Parameter(Mandatory=$true)][string] $Token,
  [Parameter(Mandatory=$true)][string] $RepoFull, # format: owner/repo, e.g. dotnet/aspire
  [string] $OutputBaseDir = ".\output",
  [int] $IssuePageSize = 100,
  [int] $CommentPageSize = 50,
  [int] $ReactionPageSize = 50,
  [int] $MinRateRemaining = 50,
  [int] $MaxRetries = 3
)

# Parse owner/repo
if ($RepoFull -notmatch '/') { Write-Host "Repo must be in form owner/repo"; exit 1 }
$parts = $RepoFull.Split('/')
$Owner = $parts[0]
$Repo = $parts[1]
$RepoSafeName = "$Owner-$Repo"
$OutputDir = Join-Path $OutputBaseDir $RepoSafeName

function Write-Log { param($m) Write-Host $m }
function Ensure-Dir { param($d) if (-not (Test-Path $d)) { New-Item -Path $d -ItemType Directory | Out-Null } }

function Rest-GetAllPages {
  param($url, $headers)
  $page = 1
  $acc = @()
  while ($true) {
    $pagedUrl = "$url`&per_page=100&page=$page"
    try {
      $resp = Invoke-RestMethod -Uri $pagedUrl -Method Get -Headers $headers -ErrorAction Stop
    } catch {
      Write-Log ("REST call failed: {0}" -f $_.Exception.Message)
      return $null
    }
    if ($null -eq $resp -or $resp.Count -eq 0) { break }
    $acc += $resp
    if ($resp.Count -lt 100) { break }
    $page++
  }
  return $acc
}

function Rest-Get {
  param($url, $headers)
  try {
    return Invoke-RestMethod -Uri $url -Method Get -Headers $headers -ErrorAction Stop
  } catch {
    Write-Log ("REST call failed: {0}" -f $_.Exception.Message)
    return $null
  }
}

function Invoke-Gql {
  param($query, $variables)
  $body = @{ query = $query; variables = $variables } | ConvertTo-Json -Depth 12
  $h = @{ Authorization = "bearer $Token"; "User-Agent" = "pwsh-fetch-script"; Accept = "application/vnd.github.v4+json" }
  try {
    return Invoke-RestMethod -Uri "https://api.github.com/graphql" -Method Post -Headers $h -Body $body -ContentType "application/json" -ErrorAction Stop
  } catch {
    Write-Log ("GraphQL call failed: {0}" -f $_.Exception.Message)
    return $null
  }
}

function Ensure-RateLimit {
  param($rateObj)
  if ($null -eq $rateObj) { return }
  $remaining = $rateObj.remaining
  $reset = $rateObj.resetAt
  Write-Log ("Rate remaining: {0} | resetAt: {1}" -f $remaining, $reset)
  if ($remaining -lt $MinRateRemaining) {
    $resetTime = [DateTime]::Parse($reset).ToUniversalTime()
    $now = (Get-Date).ToUniversalTime()
    $sleep = [int]([Math]::Max(5, ($resetTime - $now).TotalSeconds + 5))
    Write-Log ("Low rate; sleeping {0}s until reset..." -f $sleep)
    Start-Sleep -Seconds $sleep
  }
}

# GraphQL queries
$issueQuery = @'
query(
  $owner:String!, $name:String!, $labels:[String!],
  $issuePageSize:Int!, $issueCursor:String,
  $commentPageSize:Int!, $reactionPageSize:Int!
) {
  repository(owner:$owner, name:$name) {
    issues(first:$issuePageSize, after:$issueCursor, states:OPEN, labels:$labels) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        url
        createdAt
        updatedAt
        body
        author { login }
        labels(first:50) { nodes { name } }
        reactions(first:$reactionPageSize) { totalCount nodes { content user { login } createdAt } pageInfo { hasNextPage endCursor } }
        comments(first:$commentPageSize) { totalCount nodes { id author { login } createdAt updatedAt body reactions(first:$reactionPageSize) { totalCount nodes { content user { login } createdAt } pageInfo { hasNextPage endCursor } } } pageInfo { hasNextPage endCursor } }
      }
    }
  }
  rateLimit { remaining resetAt limit }
}
'@

$commentsPageQuery = @'
query($owner:String!, $name:String!, $number:Int!, $pageSize:Int!, $cursor:String, $reactionPageSize:Int!) {
  repository(owner:$owner, name:$name) {
    issue(number:$number) {
      comments(first:$pageSize, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          author { login }
          createdAt
          updatedAt
          body
          reactions(first:$reactionPageSize) { totalCount nodes { content user { login } createdAt } pageInfo { hasNextPage endCursor } }
        }
      }
    }
  }
  rateLimit { remaining resetAt }
}
'@

$reactionsPageQuery = @'
query($owner:String!, $name:String!, $number:Int!, $pageSize:Int!, $cursor:String) {
  repository(owner:$owner, name:$name) {
    issue(number:$number) {
      reactions(first:$pageSize, after:$cursor) { pageInfo { hasNextPage endCursor } nodes { content user { login } createdAt } totalCount }
    }
  }
  rateLimit { remaining resetAt }
}
'@

$commentReactionsByIdQuery = @'
query($commentId:ID!, $pageSize:Int!, $cursor:String) {
  node(id:$commentId) {
    ... on IssueComment {
      reactions(first:$pageSize, after:$cursor) { pageInfo { hasNextPage endCursor } nodes { content user { login } createdAt } totalCount }
    }
  }
  rateLimit { remaining resetAt }
}
'@

# Prepare output directory
Ensure-Dir $OutputBaseDir
Ensure-Dir $OutputDir
$restHeaders = @{ Authorization = "bearer $Token"; "User-Agent" = "pwsh-script" }

# 1) Get labels (REST, paged)
Write-Log "Listing repository labels..."
$labelsUrl = "https://api.github.com/repos/$Owner/$Repo/labels?"
$labels = Rest-GetAllPages -url $labelsUrl -headers $restHeaders
if ($null -eq $labels) { throw "Failed to list labels" }

$areaLabels = $labels | Where-Object { $_.name -like "area-*" } | ForEach-Object { $_.name }
Write-Log ("Found {0} labels with 'area-' prefix" -f $areaLabels.Count)

# 2) For each area label, process if not already present
foreach ($labelName in $areaLabels) {
  $safeName = ($labelName -replace '[\/\\:\*\?""\<\>\|]','_') -replace '\s+','_'
  $outFile = Join-Path $OutputDir ("{0}.issues.json" -f $safeName)

  if (Test-Path $outFile) {
    Write-Log ("Skipping {0} because {1} exists" -f $labelName, $outFile)
    continue
  }

  Write-Log ("Processing label: {0}" -f $labelName)

  # Use REST search to get expected total count for this label (works for public repos)
  $q = [System.Uri]::EscapeDataString("repo:$Owner/$Repo is:issue is:open label:""$labelName""")
  $searchUrl = "https://api.github.com/search/issues?q=$q"
  $searchResp = Rest-Get -url $searchUrl -headers $restHeaders
  if ($null -eq $searchResp) {
    Write-Log "Search failed; skipping label for now"
    continue
  }
  $expectedTotal = $searchResp.total_count
  Write-Log ("Expected open issues with label {0}: {1}" -f $labelName, $expectedTotal)

  # Fetch all issues via GraphQL, paged
  $allIssues = @()
  $after = $null
  $hasNext = $true
  $attempt = 0
  $adaptiveIssuePageSize = $IssuePageSize
  $failed = $false

  while ($hasNext) {
    $attempt++
    if ($attempt -gt $MaxRetries) { Write-Log "Max retries reached on page; aborting label."; $failed = $true; break }

    $vars = @{ owner=$Owner; name=$Repo; labels=@($labelName); issuePageSize=$adaptiveIssuePageSize; issueCursor=$after; commentPageSize=$CommentPageSize; reactionPageSize=$ReactionPageSize }
    $resp = Invoke-Gql -query $issueQuery -variables $vars
    if ($null -eq $resp) {
      Write-Log ("GraphQL page failed on attempt {0}; reducing page size and retrying." -f $attempt)
      $adaptiveIssuePageSize = [int]([Math]::Max(10, [Math]::Floor($adaptiveIssuePageSize/2)))
      Start-Sleep -Seconds 2
      continue
    }

    if ($resp.errors) {
      $resp.errors | ConvertTo-Json -Depth 8 |  Out-File -FilePath ("./graphql-errors-{0}.json" -f $safeName) -Encoding UTF8
      Write-Log ("GraphQL returned errors; saved ./graphql-errors-{0}.json; aborting label." -f $safeName)
      $failed = $true
      break
    }

    if ($null -eq $resp.data) {
      $resp | ConvertTo-Json -Depth 10 | Out-File -FilePath (".\graphql-debug-{0}.json" -f $safeName) -Encoding UTF8
      Write-Log ("GraphQL returned no data; saved ./graphql-debug-{0}.json; aborting label." -f $safeName)
      $failed = $true
      break
    }

    Ensure-RateLimit $resp.data.rateLimit

    $issuesBlock = $resp.data.repository.issues
    $nodes = $issuesBlock.nodes
    Write-Log ("Fetched issue page: {0} nodes (hasNextPage={1})" -f $nodes.Count, $issuesBlock.pageInfo.hasNextPage)

    foreach ($node in $nodes) {
      $issue = @{ 
        number = $node.number
        title = $node.title
        url = $node.url
        createdAt = $node.createdAt
        updatedAt = $node.updatedAt
        body = $node.body
        author = if ($node.author) { $node.author.login } else { $null }
        labels = @()
        reactions = @{ totalCount = 0; nodes = @(); hasNextPage = $false; endCursor = $null }
        comments = @{ totalCount = 0; nodes = @(); hasNextPage = $false; endCursor = $null }
      }
      if ($node.labels -and $node.labels.nodes) { foreach ($l in $node.labels.nodes) { $issue.labels += $l.name } }
      if ($node.reactions) {
        $issue.reactions.totalCount = $node.reactions.totalCount
        if ($node.reactions.nodes) {
          foreach ($r in $node.reactions.nodes) {
            $issue.reactions.nodes += @{ content = $r.content; user = if ($r.user) { $r.user.login } else { $null }; createdAt = $r.createdAt }
          }
        }
        $issue.reactions.hasNextPage = $node.reactions.pageInfo.hasNextPage
        $issue.reactions.endCursor = $node.reactions.pageInfo.endCursor
      }
      if ($node.comments) {
        $issue.comments.totalCount = $node.comments.totalCount
        if ($node.comments.nodes) {
          foreach ($c in $node.comments.nodes) {
            $cObj = @{ id = $c.id; author = if ($c.author) { $c.author.login } else { $null }; createdAt = $c.createdAt; updatedAt = $c.updatedAt; body = $c.body; reactions = @{ totalCount = 0; nodes = @(); hasNextPage = $false; endCursor = $null } }
            if ($c.reactions) {
              $cObj.reactions.totalCount = $c.reactions.totalCount
              if ($c.reactions.nodes) {
                foreach ($cr in $c.reactions.nodes) {
                  $cObj.reactions.nodes += @{ content = $cr.content; user = if ($cr.user) { $cr.user.login } else { $null }; createdAt = $cr.createdAt }
                }
              }
              $cObj.reactions.hasNextPage = $c.reactions.pageInfo.hasNextPage
              $cObj.reactions.endCursor = $c.reactions.pageInfo.endCursor
            }
            $issue.comments.nodes += $cObj
          }
        }
        $issue.comments.hasNextPage = $node.comments.pageInfo.hasNextPage
        $issue.comments.endCursor = $node.comments.pageInfo.endCursor
      }
      $allIssues += $issue
    }

    $hasNext = $issuesBlock.pageInfo.hasNextPage
    $after = if ($hasNext) { $issuesBlock.pageInfo.endCursor } else { $null }

    # Page additional comment/reaction pages for issues from this batch
    foreach ($iss in $allIssues | Where-Object { ($_.comments.hasNextPage -eq $true) -or ($_.reactions.hasNextPage -eq $true) }) {

      if ($iss.comments.hasNextPage) {
        $cAfter = $iss.comments.endCursor
        do {
          $v = @{ owner=$Owner; name=$Repo; number=[int]$iss.number; pageSize=$CommentPageSize; cursor=$cAfter; reactionPageSize=$ReactionPageSize }
          $cResp = Invoke-Gql -query $commentsPageQuery -variables $v
          if ($null -eq $cResp) { Start-Sleep -Seconds 2; continue }
          if ($cResp.errors) { $cResp | ConvertTo-Json -Depth 8 | Out-File -FilePath (".\graphql-debug-comments-{0}.json" -f $safeName) -Encoding UTF8; Write-Log "Comments paging error; skipping label"; $failed = $true; break }
          Ensure-RateLimit $cResp.data.rateLimit
          $pageInfo = $cResp.data.repository.issue.comments.pageInfo
          $cNodes = $cResp.data.repository.issue.comments.nodes
          foreach ($c in $cNodes) {
            $cObj = @{ id = $c.id; author = if ($c.author) { $c.author.login } else { $null }; createdAt = $c.createdAt; updatedAt = $c.updatedAt; body = $c.body; reactions = @{ totalCount = 0; nodes = @(); hasNextPage = $false; endCursor = $null } }
            if ($c.reactions) {
              $cObj.reactions.totalCount = $c.reactions.totalCount
              if ($c.reactions.nodes) { foreach ($cr in $c.reactions.nodes) { $cObj.reactions.nodes += @{ content=$cr.content; user = if ($cr.user) { $cr.user.login } else { $null }; createdAt = $cr.createdAt } } }
              $cObj.reactions.hasNextPage = $c.reactions.pageInfo.hasNextPage
              $cObj.reactions.endCursor = $c.reactions.pageInfo.endCursor
            }
            $iss.comments.nodes += $cObj
          }
          $cHasNext = $pageInfo.hasNextPage
          $cAfter = if ($cHasNext) { $pageInfo.endCursor } else { $null }
        } while ($cHasNext)
        if ($failed) { break }
        $iss.comments.hasNextPage = $false
      }

      if ($iss.reactions.hasNextPage) {
        $rAfter = $iss.reactions.endCursor
        do {
          $v = @{ owner=$Owner; name=$Repo; number=[int]$iss.number; pageSize=$ReactionPageSize; cursor=$rAfter }
          $rResp = Invoke-Gql -query $reactionsPageQuery -variables $v
          if ($null -eq $rResp) { Start-Sleep -Seconds 2; continue }
          if ($rResp.errors) { $rResp | ConvertTo-Json -Depth 8 | Out-File -FilePath (".\graphql-debug-reactions-{0}.json" -f $safeName) -Encoding UTF8; Write-Log "Reactions paging error; skipping label"; $failed = $true; break }
          Ensure-RateLimit $rResp.data.rateLimit
          $rNodes = $rResp.data.repository.issue.reactions.nodes
          foreach ($rn in $rNodes) { $iss.reactions.nodes += @{ content = $rn.content; user = if ($rn.user) { $rn.user.login } else { $null }; createdAt = $rn.createdAt } }
          $rPageInfo = $rResp.data.repository.issue.reactions.pageInfo
          $rHasNext = $rPageInfo.hasNextPage
          $rAfter = if ($rHasNext) { $rPageInfo.endCursor } else { $null }
        } while ($rHasNext)
        if ($failed) { break }
        $iss.reactions.hasNextPage = $false
      }

      # comment-level reactions
      foreach ($c in $iss.comments.nodes | Where-Object { $_.reactions.hasNextPage -eq $true }) {
        $crAfter = $c.reactions.endCursor
        do {
          $v = @{ commentId = $c.id; pageSize = $ReactionPageSize; cursor = $crAfter }
          $crResp = Invoke-Gql -query $commentReactionsByIdQuery -variables $v
          if ($null -eq $crResp) { Start-Sleep -Seconds 2; continue }
          if ($crResp.errors) { $crResp | ConvertTo-Json -Depth 8 | Out-File -FilePath (".\graphql-debug-comment-reactions-{0}.json" -f $safeName) -Encoding UTF8; Write-Log "Comment reactions paging error; skipping label"; $failed = $true; break }
          Ensure-RateLimit $crResp.data.rateLimit
          $rNodes = $crResp.data.node.reactions.nodes
          foreach ($rn in $rNodes) { $c.reactions.nodes += @{ content = $rn.content; user = if ($rn.user) { $rn.user.login } else { $null }; createdAt = $rn.createdAt } }
          $crPageInfo = $crResp.data.node.reactions.pageInfo
          $crHasNext = $crPageInfo.hasNextPage
          $crAfter = if ($crHasNext) { $crPageInfo.endCursor } else { $null }
        } while ($crHasNext)
        if ($failed) { break }
        $c.reactions.hasNextPage = $false
      }

      if ($failed) { break }
    } # end per-issue extra paging

    if ($failed) { break }
    $attempt = 0
  } # end while hasNext

  # Verify completeness and save if good
  if (-not $failed) {
    if ($expectedTotal -eq $allIssues.Count) {
      Write-Log ("Successfully retrieved {0} issues for label {1}. Saving to {2}" -f $allIssues.Count, $labelName, $outFile)
      $allIssues | ConvertTo-Json -Depth 20 | Out-File -FilePath $outFile -Encoding UTF8
    } else {
      Write-Log ("Mismatch for label {0}: expected {1} but retrieved {2}. Will not save; debug files created." -f $labelName, $expectedTotal, $allIssues.Count)
      $allIssues | ConvertTo-Json -Depth 20 | Out-File -FilePath ".\partial-{0}.json" -Encoding UTF8 -ErrorAction SilentlyContinue
    }
  } else {
    Write-Log ("Failed to retrieve all data for label {0}; skipping save." -f $labelName)
  }

} # end foreach label

Write-Log "Done processing labels. Saved files are in $OutputDir (only fully-complete label files were written)."