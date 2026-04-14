# bosszp-selenium

使用 **Python + [DrissionPage](https://www.drissionpage.cn/)** 控制 Chrome（**不依赖 chromedriver**），在 **Boss 直聘** 上按 **关键词 + 上海 + 在校生/实习** 抓取职位，可选进入详情页抓取 **岗位 JD**。默认写入本地 **Parquet**（`data/raw/boss_jobs/dt=…/part-*.parquet`）；可选 **`--sink postgres`** 或 **`--sink both`** 写入 **PostgreSQL**（`finance.job_info`，含 `job_jd` 列）。详见 [PARQUET_SOLUTION.md](PARQUET_SOLUTION.md)。

---

## 功能概览

| 能力 | 说明 |
|------|------|
| 搜索列表 | 新版搜索页路径 **`/web/geek/jobs`**（可改回 `job`） |
| 列表解析 | 职位卡片字段：标题、地点、公司、标签、薪资、实习要求等 |
| 反爬与节奏 | 随机间隔、分段滚动、有界面过安全验证；**勿高频滥用** |
| 评价块过滤 | 列表内嵌入的「评价」等非职位块（无 `job_detail` 链）不参与解析 |
| URL 去重 | 同一职位因 DOM 嵌套被 XPath 命中多次时，**按详情链接 URL 去重** |
| 详情 JD | `--fetch-jd`：新标签打开 **`job_detail` 对应详情页**，抽取正文并清洗 |
| 截断条数 | `--max-cards N`：只处理过滤、去重后的前 N 条（便于测试） |

---

## 核心逻辑（从打开浏览器到落盘）

以下为 **`boss_selenium.py`** 的主线设计，便于二次开发与排障。

1. **启动浏览器**  
   - 默认复用本机 Chrome 用户目录（cookie / 登录态），**须先关闭所有 Chrome 窗口**。  
   - 通过 **`--user-data-dir` + `--profile-directory`** 指定配置，避免多账号时出现「选用户」页卡住（见下文采坑）。  
   - 临时干净环境：`BOSS_FRESH_PROFILE=1`（`auto_port`，无登录）。

2. **预热**  
   - 先访问 `https://www.zhipin.com/` 停留数秒，再进入搜索 URL，降低一上来就被拦的概率。

3. **搜索 URL**  
   - `build_search_url`：`/web/geek/jobs?query=...&city=101020100&experience=108&page=`  
   - 与浏览器里常见的 **`jobs`（复数）** 一致；旧版单数路径可用环境变量切换。

4. **等待列表壳**  
   - 轮询：安全验证标题 → 或 `job_detail` 链接数 → 或空结果文案；**不在安全页上高频打 DOM**。

5. **安全验证**  
   - 有界面：终端提示，用户在浏览器内完成极验后回车；**不再对当前 URL 做额外 `get` 以免循环验证**。  
   - 无头：无法过检，会直接失败。

6. **列表区滚动 `scroll_job_list_load_more`**  
   - 分段 `scroll.down`，触发懒加载更多卡片。  
   - 可选：统计「评价型」嵌入块，≥2 时停止继续下拉（实际站点表现不一，**以「能继续加载」为主**）。  
   - **`--max-cards` 较小时**：启用 **quick** 模式（更少轮次 + `job_detail` 链接数达到 `max_cards+12` 即停），避免为抓十几条却滚出几十条 DOM。

7. **取卡片 `_find_job_cards`**  
   - 多路 CSS/XPath；**宽泛的 `//li[.//a[…job_detail…]]` 若命中过多会跳过**，避免嵌套 `li` 爆炸。

8. **过滤与去重**  
   - **`filter_real_job_cards`**：无 `job_detail` 的节点视为评价/运营位，丢弃。  
   - **`dedupe_job_cards_by_detail_url`**：同一详情 URL 只保留一条，与「列表点进去即该 JD」一致。

9. **解析 `parse_job_card`**  
   - 从卡片 DOM 取字段；**列表标题薪资常为图标字体（PUA）**，终端可能显示为方框，属 Boss 前端策略。

10. **可选 JD `fetch_job_jd_in_new_tab`**  
    - 从卡片取 **第一条** `a[href*='job_detail']`，**新标签**打开详情，`activate_tab` 回列表。  
    - 正文优先 `.job-sec-text` 等选择器；再经 **`_clean_jd_text`**：去私用区、顶栏 **`【薪资】` 明文**、去常见「直聘 / boss」插字。  
    - 入库字段 **`job_jd`**（见 `schema.sql`）。

11. **落盘**  
    - 非 `--dry-run` 时：默认 **`--sink parquet`** 按页批量写入 Parquet；**`--sink postgres`** 时逐条 `INSERT` 到 `finance.job_info`；**`--sink both`** 两者都做。  
    - 进程内去重：优先 **`detail_url`**，否则回退为 `(标题, 公司, 地点)`。

---

## 环境与依赖

```cmd
pip install -r requirements.txt
```

当前依赖：**DrissionPage**、**pandas**、**pyarrow**、**psycopg2-binary**（PostgreSQL 可选；无 Selenium / undetected-chromedriver）。

- **Chrome**：使用本机已安装的 Google Chrome；DrissionPage 通过 CDP 连接，**无需单独配置 chromedriver**。  
- **PostgreSQL 建表 / 补列**：见 [schema.sql](schema.sql)（含 **`job_jd`** 的 `CREATE` 与 `ADD COLUMN IF NOT EXISTS`）。

---

## 配置说明

### 环境变量（数据库）

| 变量 | 说明 | 默认 |
|------|------|------|
| `PGHOST` | 主机 | `localhost` |
| `PGPORT` | 端口 | `5432` |
| `PGUSER` | 用户 | `postgres` |
| `PGPASSWORD` | 密码 | `pg621` |
| `PGDATABASE` | 库名 | `postgres` |
| `PGJOB_SCHEMA` | schema | `finance` |

### 环境变量（浏览器与列表）

| 变量 | 说明 | 默认 |
|------|------|------|
| `BOSS_CHROME_USER_DATA` | Chrome `User Data` 根目录 | `%LOCALAPPDATA%\Google\Chrome\User Data` |
| `BOSS_CHROME_PROFILE` | 配置文件夹名，如 `Default`、`Profile 1` | `Default` |
| `BOSS_FRESH_PROFILE` | `1`：不用系统配置，临时 `auto_port` 配置 | `0` |
| `BOSS_SCRAPER_HEADLESS` | `1` 无头 | **`0`（建议有界面）** |
| `BOSS_SCRAPER_WAIT` | 基础超时（秒） | `25` |
| `BOSS_SCRAPER_RESTART_EVERY` | 每处理多少个关键词重启浏览器 | `5` |
| `BOSS_SCRAPER_MAX_PAGES` | 每关键词最大页数（与 `--max-pages` 二选一） | `10` |
| `BOSS_GEEK_SEARCH_PATH` | URL 段：`jobs` 或 `job` | `jobs` |
| `BOSS_WAIT_LIST_MAX` | 等待列表出现的最长时间（秒） | `90` |
| `BOSS_WAIT_LIST_POLL` | 等待列表时的轮询间隔（秒） | `5` |
| `BOSS_LIST_SCROLL_MAX` | 列表分段下拉最大轮数 | `40` |
| `BOSS_LIST_SCROLL_PIXEL` | 每轮向下滚动像素 | `480` |

### 请求间隔（秒，均为 `最小,最大` 随机区间）

与 `BOSS_SLEEP_MULT` 联用；启动日志会打印当前值。

| 变量 | 含义 | 默认 |
|------|------|------|
| `BOSS_SLEEP_MULT` | 全局乘数 | `1` |
| `BOSS_SLEEP_AFTER_NAV` | 每次打开搜索 URL 后 | `8,18` |
| `BOSS_SLEEP_AFTER_SHELL` | 列表就绪后再停一会 | `3,8` |
| `BOSS_SLEEP_SCROLL` | （保留，列表滚动内有独立短间隔） | `4,10` |
| `BOSS_SLEEP_BETWEEN_PAGES` | 翻页前 | `22,48` |
| `BOSS_SLEEP_BETWEEN_KEYWORDS` | 关键词之间 | `72,150` |
| `BOSS_SLEEP_AFTER_RESTART` | 重启浏览器后 | `22,45` |
| `BOSS_SLEEP_PER_JOB_ROW` | 每条卡片解析间隔 | `0.12,0.45` |
| `BOSS_SLEEP_EVERY_N_JOBS` | 每 N 条批量多歇（0 关闭） | `8` |
| `BOSS_SLEEP_BATCH_PAUSE` | 批量歇的时长 | `5,14` |

---

## 命令行参数

| 参数 | 说明 |
|------|------|
| `--dry-run` | 不写 Parquet/库，仅日志/打印 |
| `--sink` | `parquet`（默认） / `postgres` / `both` |
| `--output-dir` | Parquet 根目录，默认 `data/raw/boss_jobs` |
| `--visible` / `--headless` | 强制有界面 / 无头（二选一；默认读环境变量） |
| `--keywords a,b` | 逗号分隔关键词，覆盖默认 `SEARCH_TASKS` |
| `--max-pages N` | 每关键词最多 N 页；`0` 表示用 `BOSS_SCRAPER_MAX_PAGES` |
| `--max-cards N` | 每页只处理前 N 条**真实职位**（过滤+URL 去重后截断） |
| `--fetch-jd` | 打开详情页抓取 JD 写入 `job_jd` |
| `--max-jd N` | 每页最多抓 N 条 JD；不配时：若指定了 `--max-cards` 则与其相同，否则为 `5` |

**推荐测试（前 15 条 + 全部 JD）**：

```cmd
python boss_selenium.py --dry-run --visible --keywords 量化实习 --max-pages 1 --max-cards 15 --fetch-jd
```

**冒烟（仅列表、1 页）**：

```cmd
python boss_selenium.py --dry-run --visible --keywords 量化实习 --max-pages 1
```

日志：控制台 + **`boss_scraper.log`**（UTF-8）。PowerShell 若中文乱码可先执行：`chcp 65001`。

---

## 数据库表

表 **`finance.job_info`** 字段见 [schema.sql](schema.sql)。  
若表在增加 **`job_jd`** 之前已存在，在 `psql` 中执行一次 `schema.sql` 末尾的：

`ALTER TABLE finance.job_info ADD COLUMN IF NOT EXISTS job_jd TEXT;`

---

## 采坑记录与对应修复（重要）

以下为实际对接 Boss 与 Windows 环境时的结论，**避免再走弯路**。

### 1. chromedriver 与 Selenium 方案

- **现象**：`undetected-chromedriver` 自动下载驱动超时；或 **`InvalidSessionIdException`**，页面闪一下就到 new tab，会话被干掉。  
- **原因**：网络限制、Boss 对自动化检测、以及 **`driver_executable_path` 与 patch 行为** 等叠加。  
- **修复**：改为 **DrissionPage**，通过 **CDP 控本机 Chrome**，不再依赖 chromedriver 可执行文件。

### 2. Windows 控制台与「乱码」

- **现象**：`UnicodeEncodeError: 'gbk' codec can't encode character …`（私用区 U+E000 等）。  
- **原因**：Boss **列表薪资等用图标字体**，`innerText` 落在 Unicode **私用区**；默认 **GBK 控制台** 无法编码。  
- **修复**：启动时 **`sys.stdout/stderr.reconfigure(encoding='utf-8', errors='replace')`**。  
- **JD 薪资**：详情正文里同样是图标字时，从顶栏 **`.salary` 等** 再读一层，拼 **`【薪资】200-250元/天`** 这类明文；正文再做 **PUA 剥离** 与 **「直聘 / boss」插字** 清理（见 `_clean_jd_text`）。

### 3. 多 Chrome 用户 / 账号选择页

- **现象**：复用 `User Data` 后浏览器停在 **选用户/选配置**，脚本卡住。  
- **原因**：未指定 **`--profile-directory`**，Chrome 不知道进哪个 Profile。  
- **修复**：显式设置 **`BOSS_CHROME_USER_DATA`** + **`BOSS_CHROME_PROFILE`**（如 `Profile 1`），与日常登录 Boss 的 Profile 一致；**运行前关掉所有 Chrome**。

### 4. 搜索 URL：`/job` vs `/jobs`

- **现象**：浏览器里是 **`/web/geek/jobs`**，脚本写 **`/job`** 时，列表一直「等不到」或 DOM 与预期不符。  
- **修复**：默认路径改为 **`jobs`**，可用 **`BOSS_GEEK_SEARCH_PATH`** 改回 `job`。

### 5. 列表 XPath 命中 100+「假卡片」

- **现象**：`//li[.//a[contains(@href,'job_detail')]]` 把**一张大卡片里每个嵌套 `li`** 都算一条，去重前列数暴涨（如 135）。  
- **修复**：**按详情 URL 去重**（`dedupe_job_cards_by_detail_url`）；宽泛 XPath **命中数过大则不用**；优先 **外层 `li.job-card-wrapper`** 等选择器。

### 6. 列表里的「评价」块与空解析

- **现象**：滚动停在评价区附近、或解析到「空」/怪卡片。  
- **修复**：**无 `job_detail` 链接的节点不当作职位**；滚动以加载更多为主，**第二个评价栏** 的停滚逻辑因站点差异可能不稳定，已通过 **`--max-cards` + quick 滚动 + 链接数阈值** 控制成本。

### 7. 安全验证后不要再强行 `get` 同一 URL

- **现象**：用户手动过验证后脚本再 **`get` 一次**，又触发验证，页面狂跳。  
- **修复**：过验证后**等待自然跳转**，不再为「重试」重复加载同一搜索 URL（逻辑保留在 DrissionPage 版流程设计思路中）。

### 8. JD 与列表标签切换

- **现象**：详情用同标签 `back` 时 SPA 状态易乱。  
- **修复**：**`new_tab` 打开详情 → 抓 JD → `close` 子标签 → `activate_tab` 回列表**。

---

## 站点改版与自检

- 列表选择器集中在 **`_find_job_cards`**；解析在 **`parse_job_card`**。  
- 若列表为空：会写 **`boss_last_page.html`**。  
- 详情 JD 选择器在 **`fetch_job_jd_in_new_tab`**；改版时需对照单页 HTML 调整。

---

## 历史文件

`img.png`、`img_1.png` 为旧版（类目首页）流程示意；当前逻辑为 **关键词搜索页 + 可选详情 JD**，与旧图不完全一致。
