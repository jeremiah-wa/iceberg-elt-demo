select
    repo || '#' || cast(number as varchar) as pull_request_id,
    repo,
    number as pull_request_number,
    title,
    -- the REST API only has open/closed; a closed PR with merged_at was merged
    case when pull_request__merged_at is not null then 'MERGED' else upper(state) end as state,
    pull_request__merged_at is not null as is_merged,
    state = 'closed' as is_closed,
    user__login as author_login,
    author_association,
    comments as comments_total_count,
    reactions__total_count as reactions_total_count,
    created_at,
    updated_at,
    closed_at,
    pull_request__merged_at as merged_at,
    html_url as url

from {{ source('github', 'pull_requests') }}
