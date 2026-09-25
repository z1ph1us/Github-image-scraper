# GitHub Image Scraper

A tool to download images by keywords from GitHub — searching commits, pull requests, issues, and discussions.

---

## Sources searched

- **Issues** — GitHub REST search (`/search/issues`)
- **Pull requests** — GitHub REST search with `type:pr`
- **Commits** — GitHub REST search (`/search/commits`)
- **Discussions** — GitHub GraphQL search (`type: DISCUSSION`)

---

## Usage

**1. First run** — this creates a `config.json` template next to the script:

```
$ python3 GitHub_pic_scraper.py
```

**2. Edit `config.json`** — replace the placeholder tokens with your GitHub tokens, and change `"dog"` / `"cat"` to any keywords you like (any language, phrases work too):

```json
{
    "tokens": [
        "ghp_REPLACE_WITH_YOUR_FIRST_TOKEN",
        "ghp_REPLACE_WITH_YOUR_SECOND_TOKEN"
    ],
    "queries": [
        "dog",
        "cat"
    ]
}
```

**3. Run again** — the scraper will start searching and downloading:

```
$ python3 GitHub_pic_scraper.py
```

Downloaded images land in `Scraped_Images/`, logs in `logs/`, and `scraper_state.db` keeps track of every URL seen so re-runs only download new images.

> **Note:** If you want to re-download everything from scratch, delete `scraper_state.db` before running again — otherwise already-seen URLs will be skipped.
