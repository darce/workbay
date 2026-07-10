#!/usr/bin/env python3
"""Parse markdown task plans into stable, lane-routable work items."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

PLAN_ID_RE = re.compile(r"<!--\s*plan-id:\s*([A-Za-z0-9._:-]+)\s*-->")
HEADING_RE = re.compile(r"^(#{2,6})\s+(.*\S)\s*$")
CHECKLIST_RE = re.compile(r"^\s*-\s*\[([ xX])\]\s+(.*\S)\s*$")
LANE_TAG_RE = re.compile(r"\[lane:([A-Za-z0-9._-]+)\]")


@dataclass(frozen=True)
class ParsedPlanItem:
    text: str
    checked: bool
    heading: str
    line_start: int
    ordinal: int
    explicit_plan_id: Optional[str] = None


@dataclass(frozen=True)
class DerivedSlice:
    plan_item_id: str
    summary: str
    body: str
    heading: str
    line_start: int
    explicit_lane: Optional[str] = None


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "item"


def _strip_lane_tags(value: str) -> str:
    return re.sub(r"\s+", " ", LANE_TAG_RE.sub("", value)).strip()


def parse_task_plan(path: str | Path) -> list[ParsedPlanItem]:
    plan_path = Path(path)
    lines = plan_path.read_text().splitlines()
    current_heading = ""
    ordinal_by_heading: dict[str, int] = {}
    pending_plan_id: Optional[str] = None
    items: list[ParsedPlanItem] = []

    for line_no, raw_line in enumerate(lines, start=1):
        heading_match = HEADING_RE.match(raw_line)
        if heading_match:
            current_heading = heading_match.group(2).strip()
            pending_plan_id = None
            continue

        plan_id_match = PLAN_ID_RE.search(raw_line)
        if plan_id_match:
            pending_plan_id = plan_id_match.group(1).strip()
            continue

        checklist_match = CHECKLIST_RE.match(raw_line)
        if checklist_match:
            marker = checklist_match.group(1)
            text = checklist_match.group(2).strip()
            ordinal = ordinal_by_heading.get(current_heading, 0) + 1
            ordinal_by_heading[current_heading] = ordinal
            items.append(
                ParsedPlanItem(
                    text=text,
                    checked=marker.lower() == "x",
                    heading=current_heading,
                    line_start=line_no,
                    ordinal=ordinal,
                    explicit_plan_id=pending_plan_id,
                )
            )
            pending_plan_id = None
            continue

        if raw_line.strip():
            pending_plan_id = None

    return items


def derive_plan_item_id(item: ParsedPlanItem) -> str:
    if item.explicit_plan_id:
        return item.explicit_plan_id
    heading_slug = _slugify(item.heading or "unheaded")
    phase_match = re.match(r"phase\s+(\d+)", item.heading or "", flags=re.IGNORECASE)
    phase_slug = f"phase-{phase_match.group(1)}" if phase_match else "phase-x"
    return f"{phase_slug}::{heading_slug}::checklist_{item.ordinal}"


def normalize_plan_item(item: ParsedPlanItem) -> DerivedSlice:
    explicit_lane_match = LANE_TAG_RE.search(item.text)
    explicit_lane: Optional[str] = explicit_lane_match.group(1).strip() if explicit_lane_match else None
    summary = _strip_lane_tags(item.text)
    return DerivedSlice(
        plan_item_id=derive_plan_item_id(item),
        summary=summary,
        body=summary,
        heading=item.heading,
        line_start=item.line_start,
        explicit_lane=explicit_lane,
    )


def map_plan_item_to_lane(item: DerivedSlice, *, manifest: dict[str, Any]) -> Optional[str]:
    lanes = manifest.get("lanes", {})
    if not isinstance(lanes, dict):
        return None
    lane_ids = {lane_id for lane_id in lanes if isinstance(lane_id, str)}

    if item.explicit_lane and item.explicit_lane in lane_ids:
        return item.explicit_lane

    heading_map = manifest.get("heading_to_lane", {})
    if isinstance(heading_map, dict):
        heading_lane = heading_map.get(item.heading)
        if isinstance(heading_lane, str) and heading_lane in lane_ids:
            return heading_lane

    hint_matches: list[str] = []
    for raw_hint in manifest.get("plan_routing_hints", []):
        if not isinstance(raw_hint, dict):
            continue
        lane_id = raw_hint.get("lane")
        if not isinstance(lane_id, str) or lane_id not in lane_ids:
            continue
        heading = raw_hint.get("heading")
        if isinstance(heading, str) and heading.strip() and heading.strip() != item.heading:
            continue
        text_prefix = raw_hint.get("text_prefix")
        if isinstance(text_prefix, str) and text_prefix.strip():
            if item.summary.startswith(text_prefix.strip()):
                hint_matches.append(lane_id)
            continue
        contains = raw_hint.get("contains")
        if isinstance(contains, str) and contains.strip():
            if contains.strip().lower() in item.summary.lower():
                hint_matches.append(lane_id)

    unique_matches = sorted(set(hint_matches))
    if len(unique_matches) == 1:
        return unique_matches[0]
    return None
