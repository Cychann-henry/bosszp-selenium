# boss-selenium

使用 Python + Selenium（undetected-chromedriver）在 **Boss 直聘** 上按 **关键词 + 上海 + 在校生/实习** 条件抓取职位，写入 PostgreSQL（`finance.job_info`）。

## 抓取策略（默认）

- 城市：上海（`city=101020100`）
- 经验：在校生（`experience=108`，偏实习向）
- 关键词：见 `boss_selenium.py` 中 `SEARCH_TASKS`（量化/资管、证券/金工、互联网数据与产品等）；可用 `--keywords` 覆盖

## 1. 环境准备

**安装依赖**

```cmd
pip install -r requirements.txt
```

**PostgreSQL 建表**

见 [schema.sql](schema.sql)，默认库 `postgres`、schema `finance`。

**连接参数（环境变量）**

| 变量 | 说明 | 默认 |
|------|------|------|
| `PGHOST` | 主机 | `localhost` |
| `PGPORT` | 端口 | `5432` |
| `PGUSER` | 用户 | `postgres` |
| `PGPASSWORD` | 密码 | `pg621` |
| `PGDATABASE` | 库名 | `postgres` |
| `PGJOB_SCHEMA` | 表所在 schema | `finance` |

**Chrome / chromedriver**

默认路径（与 `news7.0.py` 一致）：

`D:\Desktop\必然\CS转生\chromedriver-win64-134\chromedriver-win64\chromedriver.exe`

覆盖：`set BOSS_CHROME_DRIVER_PATH=...`

**可选行为**

| 变量 | 说明 | 默认 |
|------|------|------|
| `BOSS_SCRAPER_HEADLESS` | `1` 无头 / `0` 有界面 | **`0`（默认有界面，便于过安全验证）** |
| `BOSS_SCRAPER_WAIT` | 显式等待秒数 | `25` |
| `BOSS_SCRAPER_RESTART_EVERY` | 每处理多少个**关键词**重启浏览器 | `5` |
| `BOSS_SCRAPER_MAX_PAGES` | 每个关键词最多翻页数 | `10` |
| `BOSS_WAIT_LIST_MAX` | 单次打开搜索页后，**最多等多久**直到职位列表节点出现（与「请求间隔」无关） | `90` |
| `BOSS_WAIT_LIST_POLL` | 上面等待过程中，每隔多久检查一次 DOM | `1.5` |

**请求间隔（秒，均为 `最小,最大` 随机区间，防短时间大量请求）**

启动时日志会打印当前生效的配置。

| 变量 | 含义 | 默认 |
|------|------|------|
| `BOSS_SLEEP_MULT` | 以上所有间隔统一乘数 | `1` |
| `BOSS_SLEEP_AFTER_NAV` | 每次 `driver.get` 打开 URL 后 | `8,18` |
| `BOSS_SLEEP_AFTER_SHELL` | 等待列表壳加载后再停一会 | `3,8` |
| `BOSS_SLEEP_SCROLL` | 每次滚动到底后 | `4,10` |
| `BOSS_SLEEP_BETWEEN_PAGES` | 同一关键词翻下一页前 | `22,48` |
| `BOSS_SLEEP_BETWEEN_KEYWORDS` | 两个关键词任务之间 | `72,150` |
| `BOSS_SLEEP_AFTER_RESTART` | 关闭浏览器再启动后 | `22,45` |
| `BOSS_SLEEP_PER_JOB_ROW` | 解析每条职位卡片之间 | `0.12,0.45` |
| `BOSS_SLEEP_EVERY_N_JOBS` | 每解析 N 条卡片额外歇一轮（0=关闭） | `8` |
| `BOSS_SLEEP_BATCH_PAUSE` | 上述「额外歇一轮」的时长 | `5,14` |

示例：整体再放慢一半：

```cmd
set BOSS_SLEEP_MULT=1.5
```

示例：关键词之间固定拉长到 3～6 分钟：

```cmd
set BOSS_SLEEP_BETWEEN_KEYWORDS=180,360
```

## 2. 安全验证（重要）

Boss 对自动化 / 部分 IP 会返回 **「安全验证」**（极验），**无头模式无法手动过检**。

- **默认即有界面浏览器**（`BOSS_SCRAPER_HEADLESS` 默认为 `0`），一般**不必再写 `--visible`**。
- 若你曾把环境变量设成无头，可 **`--visible`** 强制有界面，或在命令行用 **`--headless`** 显式无头。
- 终端出现「按回车继续」时，先在浏览器里过完验证、看到职位列表，再回车。
- 若列表仍为空，会生成 **`boss_last_page.html`** 便于本地排查。

## 3. 运行示例

**冒烟（单关键词、1 页、不写库；默认有界面）**

```cmd
python boss_selenium.py --dry-run --keywords 量化实习 --max-pages 1
```

**自定义多个关键词（逗号分隔）**

```cmd
python boss_selenium.py --dry-run --keywords 量化实习,数据分析实习 --max-pages 2
```

**默认全部 `SEARCH_TASKS` 并写库**

```cmd
python boss_selenium.py
```

**仅在需要后台无窗口时（易触发验证）**

```cmd
python boss_selenium.py --headless --dry-run --keywords 量化实习 --max-pages 1
```

日志：控制台 + `boss_scraper.log`。

## 4. 表结构

表 **`finance.job_info`** 见 [schema.sql](schema.sql)。

## 5. 站点改版

若「未找到职位列表」，检查 `JOB_LIST_XPATHS`、`JOB_LIST_CSS` 及 `parse_one_job` 内解析逻辑。

## 6. 历史截图

`img.png`、`img_1.png` 为旧版流程示意。
