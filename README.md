# bosszp-selenium

使用 **Python + [DrissionPage](https://www.drissionpage.cn/)** 控制 Chrome（不依赖 chromedriver），在 Boss 直聘按关键词抓取职位，并默认写入本地 **Parquet** 文件。

## 功能

- 关键词搜索（上海 + 在校生/实习）
- 职位卡片解析（公司、规模、地址、薪资、岗位等）
- 可选抓取详情页 JD（`--fetch-jd`）
- 按 `detail_url` 去重
- 按天分区写入 Parquet：`data/raw/boss_jobs/dt=YYYY-MM-DD/part-*.parquet`

## 安装

```cmd
pip install -r requirements.txt
```

## 命令行参数

- `--dry-run`：不写 Parquet，只打印日志
- `--output-dir`：Parquet 根目录（默认 `data/raw/boss_jobs`）
- `--keywords a,b`：覆盖默认关键词列表
- `--visible` / `--headless`：强制有界面/无头模式
- `--max-pages N`：每关键词最多抓取页数
- `--max-cards N`：每页最多处理真实职位数
- `--fetch-jd`：抓取详情页 JD 到 `job_jd`
- `--max-jd N`：每页最多抓取 JD 数

## 输出字段（Parquet）

主要字段包括：

- `job_company`：公司
- `job_scale`：公司规模
- `job_location`：地址
- `detail_url`：岗位 URL
- `job_title`：岗位名称
- `job_jd`：岗位 JD（开启 `--fetch-jd` 时）
- `publish_text`：岗位上架时间文案（页面可读到时）
- `crawl_time` / `crawl_date`：抓取时间

同时还会包含 `job_industry`、`job_finance`、`salary_text`、`salary_min/max/unit`、`job_experience`、`job_education`、`job_skills` 等分析字段。

## 示例

```cmd
python boss_selenium.py --dry-run --visible --keywords 量化实习 --max-pages 1
```

```cmd
python boss_selenium.py --visible --keywords 量化实习,数据分析实习 --max-pages 2 --fetch-jd
```
