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
                       ("Verified merchant directory", "discovery_complete"),
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
    lines.extend(["", f"Retained earlier offers: {int(report.get('retained_previous_offers', 0))}",
                  f"Evicted oldest offers at bounded capacity: {int(report.get('evicted_oldest_offers', 0))}"])
    directory = report.get("mall_directory", [])
    lines.extend(["", f"Observed reward malls: {len(directory)}",
                  f"Prior verified directory available: {report.get('mall_baseline_available') is True}",
                  f"New mall codes: {', '.join(report.get('new_mall_codes', [])) or 'none observed'}",
                  f"Removed mall codes: {', '.join(report.get('removed_mall_codes', [])) or 'none observed'}",
                  f"Missing allowlisted merchants: {', '.join(report.get('missing_eligible_merchants', [])) or 'none'}",
                  f"Malls needing identity review: {sum(item.get('status') == 'review_required' for item in directory)}",
                  "Directory observations do not authorize new malls or establish product-search support."])
    for query in report.get("queries", []):
        # Fields and two short title samples are sanitized by the collector.
        lines.extend(["", "```json", json.dumps({key: query.get(key) for key in
                     ("keyword", "vertical", "returned", "returned_merchant_codes", "samples")}, ensure_ascii=False), "```"])
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    path = Path(os.environ["ADPICK_COVERAGE_REPORT"])
    text = render(json.loads(path.read_text())) if path.is_file() else "Adpick collection did not start; no coverage report exists.\n"
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(text)
