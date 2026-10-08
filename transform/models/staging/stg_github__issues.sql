select
    repo || '#' || cast(number as varchar) as issue_id,
    repo,
    number as issue_number,
    title,
    upper(state) as state,
    state = 'closed' as is_closed,
    user__login as author_login,
    author_association,
    comments as comments_total_count,
    reactions__total_count as reactions_total_count,
    created_at,
    updated_at,
    closed_at,
    html_url as url

from {{ source('github', 'issues') }}
