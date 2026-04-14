#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""将职位记录批量写入按日期分区的 Parquet 文件。"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _sanitize_filename_part(s: str, max_len: int = 80) -> str:
    t = (s or "").strip()
    t = re.sub(r'[<>:"/\\|?*]', "_", t)
    t = re.sub(r"\s+", "_", t)
    if len(t) > max_len:
        t = t[:max_len].rstrip("_")
    return t or "kw"


def _dedupe_by_detail_url(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同一 detail_url 保留最后一条。"""
    out: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for r in rows:
        key = (r.get("detail_url") or "").strip()
        if not key:
            key = f"__nourl_{id(r)}"
        if key not in out:
            order.append(key)
        out[key] = r
    return [out[k] for k in order]


def write_jobs_to_parquet(
    rows: list[dict[str, Any]],
    output_dir: str,
    partition_date: str,
    keyword: str,
    file_time_hhmmss: str | None = None,
) -> Path | None:
    """
    写入 ``output_dir/dt=partition_date/part-{HHMMSS}-{keyword}.parquet``。
    若 rows 为空则返回 None。
    """
    if not rows:
        return None
    deduped = _dedupe_by_detail_url(rows)
    root = Path(output_dir)
    part_dir = root / f"dt={partition_date}"
    part_dir.mkdir(parents=True, exist_ok=True)

    ts = file_time_hhmmss or datetime.now().strftime("%H%M%S")
    fname = f"part-{ts}-{_sanitize_filename_part(keyword)}.parquet"
    out_path = part_dir / fname

    df = pd.DataFrame(deduped)
    df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
    return out_path
