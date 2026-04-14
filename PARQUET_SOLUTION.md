# Boss 职位爬虫 Parquet 存储方案

## 1. 目标与原则

本方案用于替代“爬虫强依赖在线 SQL 数据库”的方式，改为**本地文件化存储 + 后续按需分析**。

核心目标：

- 低心智负担：不要求先部署 PostgreSQL/MySQL。
- 快速落地：爬虫跑完即可得到可分析的数据文件。
- 易分析：支持 Pandas / Polars / DuckDB 直接读取。
- 可扩展：后续如需入库，可从 Parquet 再做二次导入。

适用前提：

- 数据规模中小（例如每天几百到几万条）。
- 以批处理分析为主，不依赖高并发在线写入。

---

## 2. 为什么选 Parquet

相比 JSONL：

- 列式存储，分析扫描更快。
- 压缩率高，磁盘占用更小。
- 类型信息更清晰（日期、数值字段更稳定）。

相比直接 SQL：

- 无服务依赖，无连接配置，无迁移脚本负担。
- 爬虫阶段只负责写文件，降低系统耦合。

结论：`Parquet` 非常适合“先抓数据、再分析”的场景。

---

## 3. 推荐目录结构

建议在项目根目录新增数据目录：

```text
data/
  raw/
    boss_jobs/
      dt=2026-04-14/
        part-0001.parquet
        part-0002.parquet
      dt=2026-04-15/
        part-0001.parquet
```

说明：

- `dt=YYYY-MM-DD` 为分区目录，便于按天增量分析。
- 每次抓取可生成一个或多个 `part-xxxx.parquet` 文件。

---

## 4. 字段模型（建议）

建议每条记录至少包含以下字段（可按现有 `parse_job_card` 结果映射）：

- `source`: 数据来源，固定为 `boss`.
- `keyword`: 本次抓取关键词。
- `city`: 城市（如有）。
- `job_title`: 岗位名称。
- `company_name`: 公司名。
- `salary_text`: 原始薪资文本（保留原始信息）。
- `salary_min`: 最低薪资（数值，可空）。
- `salary_max`: 最高薪资（数值，可空）。
- `salary_unit`: 薪资单位（如 `k/month`，可空）。
- `exp_text`: 经验要求原文。
- `edu_text`: 学历要求原文。
- `job_tags`: 标签列表或拼接字符串。
- `job_jd`: 职位描述正文（若启用 `--fetch-jd`）。
- `detail_url`: 职位详情链接（主去重键）。
- `company_url`: 公司链接（可空）。
- `publish_text`: 发布时间原文（可空）。
- `crawl_time`: 抓取时间戳（ISO8601）。
- `crawl_date`: 抓取日期（`YYYY-MM-DD`，用于分区）。

---

## 5. 去重与增量策略

建议采用“两层去重”：

1. 抓取过程中：继续沿用现有内存去重（如按 `detail_url`）。
2. 落盘前：对当批数据按 `detail_url` 去重一次（保留最新记录）。

长期增量建议：

- 以 `detail_url + crawl_date` 作为历史快照主键逻辑（同岗位不同日期可保留快照）。
- 若只关心“最新状态”，可在分析阶段按 `detail_url` 取最新 `crawl_time`。

---

## 6. 写入策略（推荐）

推荐 **批量写入**，而非每条实时写：

- 在内存累积本批结果（例如一个关键词或一页结束后）。
- 转 DataFrame 后一次写入 Parquet。
- 压缩建议：`snappy`（速度与体积平衡）。

文件命名建议：

- `part-{HHMMSS}-{keyword}.parquet`
- 例如：`part-142530-python.parquet`

---

## 7. 依赖与实现建议

建议在 `requirements.txt` 增加：

- `pandas`
- `pyarrow`

可选：

- `duckdb`（用于零门槛 SQL 分析 Parquet）

代码层建议新增：

- 输出参数：`--sink parquet`（可扩展为 `parquet|postgres|both`）
- 输出目录参数：`--output-dir data/raw/boss_jobs`
- 写入函数：`write_jobs_to_parquet(rows, output_dir, partition_date, keyword)`

---

## 8. 分析示例（无需数据库）

### 8.1 Pandas

```python
import pandas as pd

df = pd.read_parquet("data/raw/boss_jobs/dt=2026-04-14/*.parquet")
print(df[["job_title", "salary_text", "company_name"]].head())
```

### 8.2 DuckDB（直接 SQL 查文件）

```sql
SELECT
  keyword,
  COUNT(*) AS job_cnt
FROM read_parquet('data/raw/boss_jobs/dt=2026-04-14/*.parquet')
GROUP BY keyword
ORDER BY job_cnt DESC;
```

---

## 9. 演进路线（推荐）

阶段 1（现在）：

- 仅写 Parquet，先把抓取与分析跑通。

阶段 2（稳定后）：

- 加入质量校验（空字段率、重复率、异常值）。

阶段 3（有中台需求时）：

- 从 Parquet 批量导入 PostgreSQL（ETL），而不是爬虫直连数据库。

这样可以保持爬虫轻量，同时不牺牲未来扩展性。

---

## 10. 风险与注意事项

- Parquet 不适合高并发多进程同时写同一文件；应按批次写新文件。
- 字段类型要尽量稳定（例如薪资数值字段保持数值或空值）。
- `job_jd` 文本可能较长，建议开启压缩并按天分区。

---

## 11. 最小可执行落地清单

1. 新增依赖：`pandas`、`pyarrow`。
2. 在爬虫 CLI 新增 `--sink parquet --output-dir ...`。
3. 将每批抓取结果写入 `data/raw/boss_jobs/dt=YYYY-MM-DD/part-xxxx.parquet`。
4. 保留 `detail_url` 去重逻辑。
5. 用 Pandas 或 DuckDB 验证读取与基础统计。

完成以上 5 步后，即可摆脱“必须先上 SQL 才能分析”的前置条件。
