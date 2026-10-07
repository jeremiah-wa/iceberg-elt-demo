{# Column descriptions shared by the GitHub staging and mart models #}

{% docs github_repo %}
Repository the item belongs to, as `owner/name`, e.g. `apache/iceberg`. GitHub's response doesn't include it, so the dlt pipeline adds it.
{% enddocs %}

{% docs github_number %}
Number of the item within its repo, as shown in its URL. Issues and pull requests share one sequence per repo, so a number is never used by both.
{% enddocs %}

{% docs github_title %}
Title of the item when it was loaded.
{% enddocs %}

{% docs github_author_login %}
GitHub login of the user who opened the item.
{% enddocs %}

{% docs github_author_association %}
The author's relationship to the repo, as GitHub reports it: `OWNER`, `MEMBER`, `COLLABORATOR`, `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, `FIRST_TIMER`, `MANNEQUIN` or `NONE`.
{% enddocs %}

{% docs github_comments_total_count %}
Number of comments in the item's conversation. For pull requests this leaves out review comments on the diff.
{% enddocs %}

{% docs github_reactions_total_count %}
Number of reactions (👍, 🎉, ❤️, ...) on the item's description. Reactions on its comments aren't counted.
{% enddocs %}

{% docs github_is_closed %}
True when GitHub's state is `closed`. For pull requests, that includes merged ones.
{% enddocs %}

{% docs github_created_at %}
When the item was opened (UTC).
{% enddocs %}

{% docs github_updated_at %}
When anything on the item last changed, such as a comment, label or edit (UTC).
{% enddocs %}

{% docs github_closed_at %}
When the item was last closed (UTC). Null while it's open. For merged pull requests, this is the time of the merge.
{% enddocs %}

{% docs github_merged_at %}
When the pull request was merged (UTC). Null if it's open or was closed without merging.
{% enddocs %}

{% docs github_url %}
Link to the item on github.com.
{% enddocs %}
