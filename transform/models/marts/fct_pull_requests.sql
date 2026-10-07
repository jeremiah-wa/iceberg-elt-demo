select
    pull_request_id,
    repo,
    pull_request_number,
    title,
    author_login,
    state,
    is_merged,
    created_at,
    closed_at,
    date_diff('hour', created_at, closed_at) as hours_to_close,
    comments_total_count,
    reactions_total_count
from {{ ref('stg_github__pull_requests') }}
