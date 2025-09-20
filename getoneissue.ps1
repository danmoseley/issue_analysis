# Get the issue with reactions
$issue = gh api repos/dotnet/aspire/issues/3386 | ConvertFrom-Json

# Get comments with reactions
$comments = gh api repos/dotnet/aspire/issues/3386/comments | ConvertFrom-Json

# Combine the data
$result = @{
    url = $issue.html_url
    title = $issue.title
    created_at = $issue.created_at
    updated_at = $issue.updated_at
    author = $issue.user.login
    body = $issue.body
    issue_reactions = $issue.reactions
    comments = @($comments | ForEach-Object {
        @{
            author = $_.user.login
            created_at = $_.created_at
            body = $_.body
            reactions = $_.reactions
        }
    })
    comment_count = $comments.Count
    participants = @($issue.user.login) + @($comments | ForEach-Object { $_.user.login }) | Sort-Object -Unique
}

# Output as JSON
$result | ConvertTo-Json -Depth 5