import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

try:
    import jieba.analyse as jieba_analyse
except ImportError:
    jieba_analyse = None


SKILL_TAXONOMY: Dict[str, Dict[str, List[str]]] = {
    "编程语言": {
        "Python": [r"\bpython\b", r"python3"],
        "C++": [r"\bc\+\+\b"],
        "Java": [r"\bjava\b"],
        "R": [r"\br\b", r"\br语言\b"],
        "MATLAB": [r"\bmatlab\b"],
        "Scala": [r"\bscala\b"],
        "Go": [r"\bgolang\b", r"\bgo\b"],
        "Rust": [r"\brust\b"],
    },
    "数据工具": {
        "SQL": [r"\bsql\b", r"mysql", r"postgresql"],
        "Pandas": [r"\bpandas\b"],
        "NumPy": [r"\bnumpy\b", r"\bnp\b"],
        "Spark": [r"\bspark\b", r"pyspark"],
        "Hadoop": [r"\bhadoop\b"],
        "Excel": [r"\bexcel\b", r"vba"],
        "Wind": [r"\bwind\b", r"万得"],
        "Bloomberg": [r"\bbloomberg\b", r"彭博"],
    },
    "机器学习": {
        "机器学习": [r"机器学习", r"machine learning"],
        "深度学习": [r"深度学习", r"deep learning"],
        "统计建模": [r"统计建模", r"建模"],
        "时间序列": [r"时间序列", r"time\s*series"],
        "概率统计": [r"概率", r"统计学", r"statistics?"],
        "线性代数": [r"线性代数"],
        "最优化": [r"优化", r"optimization"],
        "随机过程": [r"随机过程", r"stochastic"],
    },
    "金融量化": {
        "因子模型": [r"因子", r"factor"],
        "Alpha": [r"\balpha\b"],
        "回测": [r"回测", r"backtest"],
        "风控": [r"风控", r"风险控制"],
        "衍生品": [r"衍生品", r"derivative"],
        "期权": [r"期权", r"option"],
        "CTA": [r"\bcta\b"],
        "高频交易": [r"高频", r"hft", r"高频交易"],
        "多因子": [r"多因子"],
        "基本面": [r"基本面"],
    },
    "框架库": {
        "PyTorch": [r"\bpytorch\b"],
        "TensorFlow": [r"\btensorflow\b", r"\btf\b"],
        "Scikit-learn": [r"scikit-?learn", r"\bsklearn\b"],
        "XGBoost": [r"\bxgboost\b"],
        "LightGBM": [r"\blightgbm\b", r"\blgbm\b"],
        "QuantLib": [r"\bquantlib\b"],
    },
}

SKILL_PATTERNS: Dict[str, List[str]] = {
    skill: pats for group in SKILL_TAXONOMY.values() for skill, pats in group.items()
}
SKILL_GROUP: Dict[str, str] = {
    skill: group for group, items in SKILL_TAXONOMY.items() for skill in items.keys()
}


def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), dtype="string")
    return df[col].fillna("").astype(str)


def _exclude_ops_jobs(df: pd.DataFrame) -> pd.DataFrame:
    title = _safe_series(df, "job_title").str.strip()
    return df.loc[~title.str.contains(r"运营", regex=True, na=False)].copy()


def _normalize_salary_month(row: pd.Series) -> float | None:
    unit = str(row.get("salary_unit", "")).strip()
    try:
        lo = float(row.get("salary_min"))
        hi = float(row.get("salary_max"))
    except Exception:
        return None
    mid = (lo + hi) / 2.0
    if unit == "yuan_per_day":
        return mid * 21.75
    if unit == "yuan_per_month":
        return mid
    return None


def _extract_skill_presence(text: str) -> Dict[str, bool]:
    s = (text or "").lower()
    return {
        skill: any(re.search(p, s, flags=re.IGNORECASE) for p in patterns)
        for skill, patterns in SKILL_PATTERNS.items()
    }


def _top_counts(series: Iterable[str], topn: int = 10) -> pd.Series:
    s = pd.Series(list(series), dtype="string").fillna("").astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return pd.Series(dtype="int64")
    return s.value_counts().head(topn)


def _infer_education(df: pd.DataFrame) -> pd.Series:
    edu_re = re.compile(r"(学历不限|本科|硕士|博士|大专|中专|高中)")
    raw = _safe_series(df, "job_education")
    tags = _safe_series(df, "job_tags")
    out = []
    for a, b in zip(raw.tolist(), tags.tolist()):
        m = edu_re.search(str(a).strip())
        if m:
            out.append(m.group(1))
            continue
        chosen = "未知"
        for part in [p.strip() for p in str(b).split(",") if p.strip()]:
            m = edu_re.search(part)
            if m:
                chosen = m.group(1)
                break
        out.append(chosen)
    return pd.Series(out, dtype="string")


def _infer_experience(df: pd.DataFrame) -> pd.Series:
    raw = _safe_series(df, "job_experience")
    tags = _safe_series(df, "job_tags")
    out = []
    for a, b in zip(raw.tolist(), tags.tolist()):
        a = str(a).strip()
        if a and a != "无":
            out.append(a)
            continue
        parts = [p.strip() for p in str(b).split(",") if p.strip()]
        picks = [p for p in parts if "天/周" in p or "个月" in p or "年" in p or "经验" in p]
        out.append(",".join(picks) if picks else (parts[0] if parts else "未知"))
    return pd.Series(out, dtype="string")


def _classify_track(title: str, keyword: str) -> str:
    t = f"{title} {keyword}".lower()
    rules = [
        ("量化交易", [r"交易", r"trading", r"高频"]),
        ("量化开发", [r"开发", r"engineer", r"工程", r"platform"]),
        ("风控建模", [r"风控", r"风险", r"信用"]),
        ("数据分析", [r"数据分析", r"商业分析", r"bi"]),
        ("金融工程", [r"金融工程", r"金工"]),
        ("投研", [r"证券研究", r"投研", r"研究员"]),
        ("量化研究", [r"量化", r"因子", r"alpha", r"策略"]),
    ]
    for label, pats in rules:
        if any(re.search(p, t, flags=re.IGNORECASE) for p in pats):
            return label
    return "其他"


def _classify_company_layer(row: pd.Series) -> str:
    company = str(row.get("job_company", "")).strip()
    finance = str(row.get("job_finance", "")).strip()
    industry = str(row.get("job_industry", "")).strip()
    if any(x in company for x in ["磐松", "明汯", "艾方", "九坤", "幻方", "宽德"]):
        return "头部量化私募"
    if any(x in company for x in ["字节", "腾讯", "阿里", "携程", "bilibili", "华为"]):
        return "大型互联网"
    if any(x in finance for x in ["上市公司", "已上市", "不需要融资"]):
        return "成熟机构"
    if any(x in industry for x in ["证券", "基金", "公募"]):
        return "券商公募"
    if any(x in industry for x in ["互联网金融", "金融科技", "投资"]):
        return "私募金融科技"
    return "其他"


def _extract_section_skills(df: pd.DataFrame) -> pd.DataFrame:
    must_kw = re.compile(r"(要求|需要|必须|具备|任职资格|任职要求)")
    bonus_kw = re.compile(r"(优先|加分|preferred|plus|熟悉.*者优先)", flags=re.I)
    rows: list[dict] = []
    for text in _safe_series(df, "job_jd").tolist():
        if not text:
            continue
        chunks = [x.strip() for x in re.split(r"[。\n；;]+", text) if x.strip()]
        for chunk in chunks:
            ctype = ""
            if must_kw.search(chunk):
                ctype = "must"
            elif bonus_kw.search(chunk):
                ctype = "bonus"
            if not ctype:
                continue
            p = _extract_skill_presence(chunk)
            for skill, hit in p.items():
                if hit:
                    rows.append({"type": ctype, "skill": skill})
    if not rows:
        return pd.DataFrame(columns=["type", "skill", "count"])
    out = pd.DataFrame(rows).value_counts(["type", "skill"]).reset_index(name="count")
    return out.sort_values(["type", "count"], ascending=[True, False]).reset_index(drop=True)


def _extract_tfidf_keywords(df: pd.DataFrame, topk: int = 25) -> pd.DataFrame:
    if jieba_analyse is None:
        return pd.DataFrame(columns=["track", "term", "weight"])
    rows = []
    for track, sub in df.groupby("track"):
        text = "\n".join(_safe_series(sub, "job_jd").tolist())
        if not text.strip():
            continue
        for term, weight in jieba_analyse.extract_tags(text, topK=topk, withWeight=True):
            if len(term.strip()) <= 1:
                continue
            rows.append({"track": track, "term": term, "weight": float(weight)})
    return pd.DataFrame(rows).sort_values(["track", "weight"], ascending=[True, False])


def _extract_template_sentences(df: pd.DataFrame, topn: int = 12) -> pd.DataFrame:
    duty, req = [], []
    for text in _safe_series(df, "job_jd").tolist():
        if not text:
            continue
        chunks = [x.strip() for x in re.split(r"[。\n；;]+", text) if x.strip()]
        for x in chunks:
            if len(x) < 8 or len(x) > 80:
                continue
            if re.search(r"(岗位职责|工作职责|负责|参与|协助)", x):
                duty.append(x)
            if re.search(r"(任职要求|职位要求|要求|需要|具备|熟悉)", x):
                req.append(x)
    rows = []
    for section, items in [("职责", duty), ("要求", req)]:
        top = _top_counts(items, topn)
        for sent, cnt in top.items():
            rows.append({"section": section, "sentence": sent, "count": int(cnt)})
    return pd.DataFrame(rows)


def build_skill_demand(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    base_text = (
        _safe_series(df, "job_title")
        + " "
        + _safe_series(df, "job_skills")
        + " "
        + _safe_series(df, "job_tags")
        + " "
        + _safe_series(df, "job_jd")
    )
    p = pd.DataFrame([_extract_skill_presence(t) for t in base_text.tolist()])
    if p.empty:
        return (
            pd.DataFrame(columns=["skill_group", "skill", "count", "ratio", "cooccurrence_top5"]),
            p,
        )
    total = len(p)
    rows = []
    for skill in sorted(SKILL_PATTERNS.keys()):
        count = int(p[skill].sum())
        co = {}
        if count > 0:
            subset = p[p[skill]]
            for other in SKILL_PATTERNS.keys():
                if other == skill:
                    continue
                co[other] = int(subset[other].sum())
        top5 = sorted(co.items(), key=lambda kv: kv[1], reverse=True)[:5]
        co_txt = ", ".join([f"{k}:{v}" for k, v in top5 if v > 0]) or "无"
        rows.append(
            {
                "skill_group": SKILL_GROUP.get(skill, "其他"),
                "skill": skill,
                "count": count,
                "ratio": round(count / total, 4),
                "cooccurrence_top5": co_txt,
            }
        )
    out = pd.DataFrame(rows).sort_values("count", ascending=False).reset_index(drop=True)
    return out, p


def build_position_tracks(df: pd.DataFrame, presence: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    x = df.copy()
    x["salary_month_mid"] = x.apply(_normalize_salary_month, axis=1)
    x["track"] = [
        _classify_track(a, b)
        for a, b in zip(_safe_series(x, "job_title").tolist(), _safe_series(x, "keyword").tolist())
    ]
    x["company_layer"] = x.apply(_classify_company_layer, axis=1)
    if not presence.empty:
        skill_cols = list(presence.columns)
        x = pd.concat([x.reset_index(drop=True), presence[skill_cols].reset_index(drop=True)], axis=1)
    rows = []
    for track, sub in x.groupby("track"):
        top_company = _top_counts(_safe_series(sub, "job_company"), 5)
        skill_counts = {}
        for skill in SKILL_PATTERNS.keys():
            if skill in sub.columns:
                skill_counts[skill] = int(sub[skill].sum())
        top_skills = sorted(skill_counts.items(), key=lambda kv: kv[1], reverse=True)[:8]
        rows.append(
            {
                "track": track,
                "count": int(len(sub)),
                "salary_month_median": float(sub["salary_month_mid"].dropna().median())
                if sub["salary_month_mid"].notna().any()
                else None,
                "top_skills": ", ".join([f"{k}:{v}" for k, v in top_skills if v > 0]) or "无",
                "top_companies": ", ".join([f"{k}:{int(v)}" for k, v in top_company.items()]) or "无",
            }
        )
    summary = pd.DataFrame(rows).sort_values("count", ascending=False).reset_index(drop=True)
    return x, summary


def build_salary_skill_company(df_enriched: pd.DataFrame) -> pd.DataFrame:
    rows = []
    s = df_enriched[df_enriched["salary_month_mid"].notna()].copy()
    for skill in SKILL_PATTERNS.keys():
        if skill not in s.columns:
            continue
        sub = s[s[skill]]
        if sub.empty:
            continue
        rows.append(
            {
                "scope": "skill_salary",
                "group": skill,
                "count": int(len(sub)),
                "salary_month_median": float(sub["salary_month_mid"].median()),
                "salary_month_mean": float(sub["salary_month_mid"].mean()),
            }
        )
    for layer, sub in s.groupby("company_layer"):
        rows.append(
            {
                "scope": "company_layer_salary",
                "group": layer,
                "count": int(len(sub)),
                "salary_month_median": float(sub["salary_month_mid"].median()),
                "salary_month_mean": float(sub["salary_month_mid"].mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(["scope", "salary_month_median"], ascending=[True, False])


def build_resume_match(df_enriched: pd.DataFrame, my_skills: list[str]) -> pd.DataFrame:
    if not my_skills:
        return pd.DataFrame(columns=["job_title", "job_company", "track", "match_score", "missing_skills"])
    myset = {s.strip().lower() for s in my_skills if s.strip()}
    rows = []
    for _, row in df_enriched.iterrows():
        req = [skill for skill in SKILL_PATTERNS.keys() if skill in df_enriched.columns and bool(row.get(skill))]
        req_lower = {x.lower() for x in req}
        hit = sorted([x for x in req if x.lower() in myset])
        miss = sorted([x for x in req if x.lower() not in myset])
        score = round(100.0 * len(hit) / max(1, len(req)), 2)
        if not req:
            score = 0.0
        rows.append(
            {
                "job_title": str(row.get("job_title", "")),
                "job_company": str(row.get("job_company", "")),
                "track": str(row.get("track", "")),
                "match_score": score,
                "required_skill_count": len(req_lower),
                "hit_skills": ",".join(hit),
                "missing_skills": ",".join(miss),
                "detail_url": str(row.get("detail_url", "")),
            }
        )
    out = pd.DataFrame(rows).sort_values(["match_score", "required_skill_count"], ascending=[False, False])
    return out.reset_index(drop=True)


def write_comprehensive_report(
    df_enriched: pd.DataFrame,
    skill_demand: pd.DataFrame,
    track_summary: pd.DataFrame,
    section_skills: pd.DataFrame,
    salary_skill: pd.DataFrame,
    tfidf_terms: pd.DataFrame,
    template_sentences: pd.DataFrame,
    resume_match: pd.DataFrame,
    out_path: Path,
) -> None:
    total = len(df_enriched)
    jd_nonempty = int(_safe_series(df_enriched, "job_jd").str.strip().ne("").sum())
    company_top = _top_counts(_safe_series(df_enriched, "job_company"), 10)
    edu_top = _top_counts(_infer_education(df_enriched), 8)
    exp_top = _top_counts(_infer_experience(df_enriched), 8)
    lines = [
        "# Quant Intern Comprehensive Report",
        "",
        "## 概览",
        f"- 总岗位数: {total}",
        f"- JD 填充数: {jd_nonempty}（{round(100 * jd_nonempty / max(1, total), 1)}%）",
        f"- 唯一公司数: {_safe_series(df_enriched, 'job_company').replace('', pd.NA).dropna().nunique()}",
        "",
        "## 维度1：硬技能需求频谱（Top 20）",
    ]
    for _, r in skill_demand.head(20).iterrows():
        lines.append(f"- [{r['skill_group']}] {r['skill']}: {int(r['count'])} ({r['ratio']:.1%})")

    lines.extend(["", "## 维度2：岗位方向聚类"])
    for _, r in track_summary.iterrows():
        lines.append(
            f"- {r['track']}: {int(r['count'])} 岗位 | 薪资中位 {r['salary_month_median']} | 技能 {r['top_skills']}"
        )

    lines.extend(["", "## 维度3：核心要求 vs 加分项（Top 12）"])
    must = section_skills[section_skills["type"] == "must"].head(12)
    bonus = section_skills[section_skills["type"] == "bonus"].head(12)
    lines.append("- 必须项:")
    for _, r in must.iterrows():
        lines.append(f"  - {r['skill']}: {int(r['count'])}")
    lines.append("- 加分项:")
    for _, r in bonus.iterrows():
        lines.append(f"  - {r['skill']}: {int(r['count'])}")

    lines.extend(["", "## 维度4/5：薪资与公司分层"])
    for _, r in salary_skill.head(20).iterrows():
        lines.append(
            f"- {r['scope']} | {r['group']}: count={int(r['count'])}, median={round(float(r['salary_month_median']), 1)}"
        )

    lines.extend(["", "## 维度6：JD 高频词（按 Track）"])
    for tr, sub in tfidf_terms.groupby("track"):
        terms = ", ".join(sub.head(12)["term"].tolist())
        lines.append(f"- {tr}: {terms}")

    lines.extend(["", "## 维度7：JD 共性模板（Top）"])
    for _, r in template_sentences.head(20).iterrows():
        lines.append(f"- [{r['section']}] {r['sentence']} ({int(r['count'])})")

    lines.extend(["", "## 市场补充分布"])
    lines.append("- 公司 Top10: " + ", ".join([f"{k}:{int(v)}" for k, v in company_top.items()]))
    lines.append("- 学历 Top8: " + ", ".join([f"{k}:{int(v)}" for k, v in edu_top.items()]))
    lines.append("- 出勤/经验 Top8: " + ", ".join([f"{k}:{int(v)}" for k, v in exp_top.items()]))

    lines.extend(["", "## 简历匹配（若提供 --my-skills）"])
    if resume_match.empty:
        lines.append("- 未提供个人技能，跳过匹配打分。")
    else:
        for _, r in resume_match.head(20).iterrows():
            lines.append(
                f"- {r['match_score']} | {r['job_company']} - {r['job_title']} | 缺口: {r['missing_skills']}"
            )

    out_path.write_text("\n".join(lines), encoding="utf-8")


def run(input_path: str, output_dir: str, my_skills: list[str]) -> None:
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"未找到输入 parquet: {src}")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(src)
    before_n = len(df)
    df = _exclude_ops_jobs(df)
    after_n = len(df)

    skill_demand, presence = build_skill_demand(df)
    enriched, track_summary = build_position_tracks(df, presence)
    section_skills = _extract_section_skills(enriched)
    salary_skill = build_salary_skill_company(enriched)
    tfidf_terms = _extract_tfidf_keywords(enriched)
    template_sentences = _extract_template_sentences(enriched)
    resume_match = build_resume_match(enriched, my_skills)

    skill_demand.to_parquet(out_dir / "skill_demand.parquet", index=False)
    track_summary.to_parquet(out_dir / "position_tracks.parquet", index=False)
    salary_skill.to_parquet(out_dir / "salary_skill_company.parquet", index=False)
    section_skills.to_parquet(out_dir / "core_bonus_requirements.parquet", index=False)
    tfidf_terms.to_parquet(out_dir / "jd_tfidf_terms.parquet", index=False)
    template_sentences.to_parquet(out_dir / "jd_template_sentences.parquet", index=False)
    resume_match.to_parquet(out_dir / "resume_match.parquet", index=False)

    report_path = out_dir / "comprehensive_report.md"
    write_comprehensive_report(
        enriched,
        skill_demand,
        track_summary,
        section_skills,
        salary_skill,
        tfidf_terms,
        template_sentences,
        resume_match,
        report_path,
    )
    print(f"输入文件: {src}")
    print(f"输出目录: {out_dir}")
    print(f"运营岗位过滤: {before_n - after_n} 条（剩余 {after_n} 条）")
    print(
        "生成: skill_demand.parquet, position_tracks.parquet, salary_skill_company.parquet, "
        "core_bonus_requirements.parquet, jd_tfidf_terms.parquet, jd_template_sentences.parquet, "
        "comprehensive_report.md"
    )
    if not my_skills:
        print("resume_match.parquet 未生成（未提供 --my-skills）")
    else:
        print("resume_match.parquet 已生成")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="量化实习岗位多维度分析：技能、方向、薪资、公司分层、JD 模板与简历匹配。"
    )
    parser.add_argument(
        "--input-path",
        default="data/processed/quant_intern/jobs_filtered.parquet",
        help="输入 parquet（默认使用处理后的唯一数据源）",
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed/quant_intern",
        help="处理后输出目录",
    )
    parser.add_argument(
        "--my-skills",
        default="",
        help="你的技能列表，逗号分隔；用于生成 resume_match.parquet",
    )
    args = parser.parse_args()
    my_skills = [x.strip() for x in (args.my_skills or "").split(",") if x.strip()]
    run(args.input_path, args.output_dir, my_skills)


if __name__ == "__main__":
    main()
