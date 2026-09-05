"""Summarize sanitized counters; no provider payload, URL, or credential output."""
import json
import os
from pathlib import Path


def render(report):
    lines = ["### Adpick Travel and Services collection", ""]
    for label, key in [("Searches completed", "queries_completed"),
                       ("Searches planned", "queries_planned"),
                       ("API attempts", "request_attempts"), ("Retries", "retries")]:
        lines.append(f"- {label}: {int(report.get(key, 0))}")
    for label, key in [("Complete upstream batch", "collection_complete"),
                       ("Verified ClickHouse publication", "publication_complete"),
                       ("Offer target reached", "coverage_target_met")]:
        lines.append(f"- {label}: {report.get(key) is True}")
    lines.extend(["", "| Vertical | Unique offers | Target |", "| --- | ---: | ---: |"])
    for vertical in ("travel", "services"):
        actual = int(report.get("offers_by_vertical", {}).get(vertical, 0))
        target = int(report.get("target_offers", {}).get(vertical, 0))
        lines.append(f"| {vertical} | {actual} | {target} |")
    lines.extend(["", "| Merchant | Unique offers |", "| --- | ---: |"])
    for merchant in ("trip_com", "nol", "myrealtrip", "kkday", "klook", "traveloka", "hotels_com", "kmong"):
        counts = report.get("merchants", {}).get(merchant)
        if counts is not None:
            lines.append(f"| {merchant} | {int(counts.get('offers', 0))} |")
    if report.get("coverage_target_met") is not True:
        lines.extend(["", "The source did not supply the target number of eligible offers. "
                      "Mall directory membership does not prove product-search support; "
                      "excluded merchants are never relabeled as travel or services."])
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    path = Path(os.environ["ADPICK_COVERAGE_REPORT"])
    text = render(json.loads(path.read_text())) if path.is_file() else "Adpick collection did not start; no coverage report exists.\n"
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(text)
