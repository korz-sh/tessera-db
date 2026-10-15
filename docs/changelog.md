---
title: Changelog
nav_order: 5
---

# Changelog

Releases are cut on GitHub — this page renders them automatically via
`jekyll-github-metadata`. Until the first release ships, the running
history lives in
[`CHANGELOG.md`](https://github.com/korz-sh/tessera-db/blob/main/CHANGELOG.md).

{% if site.github.releases and site.github.releases.size > 0 %}
{% for release in site.github.releases %}
## [{{ release.name | default: release.tag_name }}]({{ release.html_url }})
*{{ release.tag_name }} — published {{ release.published_at | date: "%Y-%m-%d" }}*

{{ release.body | markdownify }}

---
{% endfor %}
{% else %}
> No GitHub releases published yet.
>
> The pre-release history is in
> [CHANGELOG.md](https://github.com/korz-sh/tessera-db/blob/main/CHANGELOG.md)
> — it will move here automatically on the first `gh release`.
{% endif %}
