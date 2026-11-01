"""Validate metadata as data; never interpolate PR text into a shell command."""

import json
import os
import re
from pathlib import Path


def check(pr):
    errors = []
    head = pr["head"]["ref"]
    match = re.fullmatch(r"(?:feat|fix|refactor|docs|test|chore)/(SCRUM-[1-9][0-9]*)", head)
    if not match:
        errors.append("브랜치는 type/SCRUM-번호 형식이어야 합니다.")
    if pr["base"]["ref"] != "develop":
        errors.append("PR 대상은 develop이어야 합니다.")
    body = pr.get("body") or ""
    if match:
        key = match.group(1)
        text = pr["title"] + "\n" + body
        if not re.search(r"(?<![A-Z0-9-])" + key + r"(?![A-Z0-9-])", text):
            errors.append("PR 제목/본문에 브랜치의 Jira 키가 필요합니다.")
        if "https://dajaba.atlassian.net/browse/" + key not in body:
            errors.append("본문에 Jira 이슈 링크가 필요합니다.")
    if not any(label in body for label in ["What I Did", "Key Changes"]):
        errors.append("What I Did 또는 Key Changes 항목이 필요합니다.")
    if "To Reviewers" not in body:
        errors.append("To Reviewers 항목이 필요합니다.")
    return errors


if __name__ == "__main__":
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
    errors = check(event["pull_request"])
    for error in errors:
        print(error)
    raise SystemExit(bool(errors))
