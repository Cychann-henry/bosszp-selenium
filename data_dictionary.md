# Data Dictionary

本文件描述 `boss_selenium.py` 产出的 Parquet 字段结构，适用于：

- JD/岗位聚类分析
- 简历自适应匹配
- 投递方向个性化推荐

默认文件路径模式：

- `data/raw/boss_jobs/dt=YYYY-MM-DD/part-*.parquet`

处理后分析数据（推荐主数据源）：

- `data/processed/quant_intern/jobs_filtered.parquet`

## 字段总览

| 字段 | 类型 | 示例 | 说明 |
|---|---|---|---|
| `source` | string | `boss` | 数据来源标识 |
| `category` | string | `互联网` | 抓取任务一级分组 |
| `keyword` | string | `数据分析实习` | 本次检索关键词 |
| `city` | string | `上海` | 从 `job_location` 拆出的城市 |
| `job_title` | string | `数据分析实习生` | 岗位名称 |
| `province` | string | `上海` | 省份（由城市映射） |
| `job_location` | string | `上海·浦东新区` | 工作地址原文 |
| `job_company` | string | `某科技公司` | 公司名称 |
| `job_industry` | string | `互联网` | 行业标签（公司标签第 1 位） |
| `job_finance` | string | `B轮` | 融资阶段（公司标签第 2 位） |
| `job_scale` | string | `100-499人` | 公司规模（公司标签第 3 位） |
| `job_welfare` | string | `免费班车,下午茶` | 福利文案 |
| `salary_text` | string | `200-250元/天` | 薪资原始文本 |
| `salary_min` | float nullable | `200.0` | 解析出的最低薪资（单位见 `salary_unit`） |
| `salary_max` | float nullable | `250.0` | 解析出的最高薪资（单位见 `salary_unit`） |
| `salary_unit` | string nullable | `yuan_per_day` | 薪资单位（如 `yuan_per_day`/`yuan_per_month`） |
| `job_experience` | string | `在校/应届` | 经验要求 |
| `job_education` | string | `本科` | 学历要求 |
| `job_skills` | string | `Python,SQL,Tableau` | 技能标签拼接 |
| `job_tags` | string | `在校/应届,本科,Python` | 原标签全量拼接 |
| `job_jd` | string | `岗位职责...` | 职位描述正文（需 `--fetch-jd`） |
| `detail_url` | string | `https://www.zhipin.com/job_detail/...` | 岗位详情链接，核心去重键 |
| `company_url` | string | `https://www.zhipin.com/gongsir/...` | 公司页链接（可能为空） |
| `publish_text` | string | `2天前` | 岗位上架/发布时间原文（可能为空） |
| `crawl_time` | string (ISO8601) | `2026-04-14T14:25:30` | 抓取时间戳 |
| `crawl_date` | string (`YYYY-MM-DD`) | `2026-04-14` | 抓取日期（分区字段） |

## 关键字段建议

- 去重：优先使用 `detail_url`
- 时间分析：使用 `crawl_time` / `crawl_date`；若关注“岗位新鲜度”结合 `publish_text`
- JD 语义分析：使用 `job_jd`（未开启 `--fetch-jd` 时可能为空）
- 薪资分析：优先 `salary_min`/`salary_max`，回退 `salary_text`

## 质量注意事项

- `publish_text` 为页面文案，不保证统一格式
- `salary_*` 解析失败会为 `null`
- `company_url`、`job_jd` 可能为空（取决于页面结构和抓取参数）

## JD 回填说明

- 可使用 `python boss_selenium.py --backfill-jd --input "<parquet_path>"` 对已有 parquet 就地回填 `job_jd`
- 当前 JD 抓取采用固定标签页复用（重复导航 URL），不再每条 JD 新建/关闭标签页
- 不启用窗口移屏/最小化方案（作为备选策略保留）
