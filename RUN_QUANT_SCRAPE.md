# 量化实习抓取运行手册

本文档用于说明如何稳定运行“量化实习相关岗位抓取”，并将结果写入 Parquet。

## 1. 前置条件

- 已安装 Python（建议 3.10+）
- 已安装依赖：

```cmd
pip install -r requirements.txt
```

- 本机可正常打开 Boss 直聘
- 建议使用有界面模式（便于处理安全验证）

## 2. 标准运行命令（推荐）

下面这条命令用于提高覆盖率，适合正式抓取量化相关岗位：

```cmd
python boss_selenium.py --visible --keywords "量化实习,量化研究员实习,量化研究实习,量化开发实习,量化交易实习,量化策略实习,因子研究实习,Alpha研究实习,资产管理实习,投资实习,金融工程实习,风控建模实习,私募实习,对冲基金实习,证券研究实习" --max-pages 3 --max-cards 0 --output-dir "data/raw/boss_jobs"
```

参数含义：

- `--visible`：有界面运行（推荐）
- `--keywords`：量化方向关键词列表
- `--max-pages 3`：每个关键词最多抓取 3 页，提高覆盖率
- `--max-cards 0`：每页处理全部真实职位（0 = 不限）
- `--output-dir`：Parquet 输出根目录

## 3. 输出位置与文件命名

输出目录按日期分区：

- `data/raw/boss_jobs/dt=YYYY-MM-DD/`

文件命名示例：

- `part-151456-量化实习_p1.parquet`
- `part-152510-资产管理实习_p1.parquet`

## 4. 边界条件建议（非常重要）

为兼顾覆盖率和稳定性，建议使用以下策略：

1. 提升翻页深度：`--max-pages 3`
2. 页面全量采集：`--max-cards 0`
3. 去掉噪声关键词（如 `金工实习`,`QD实习`,`QR实习`）
4. 若单次任务太久，采用分批关键词运行（见第 6 节）

## 5. 常用运行模式

### 5.1 仅验证流程（不落盘）

```cmd
python boss_selenium.py --dry-run --visible --keywords "量化实习,量化研究员实习,量化开发实习" --max-pages 1 --max-cards 20
```

### 5.2 抓取并包含 JD（更慢）

```cmd
python boss_selenium.py --visible --keywords "量化实习,量化研究员实习,量化开发实习" --max-pages 2 --max-cards 0 --fetch-jd --max-jd 10
```

说明：`--fetch-jd` 会打开详情页，速度明显变慢。
当前实现为**固定标签页复用**：只创建一个 JD 标签页并反复切换 URL，不再每条 JD 新建/关闭标签页，可减少抢焦点中断。

### 5.3 对已有数据补抓 JD（推荐）

用于你已经有 parquet 数据，但 `job_jd` 为空的情况（例如先抓列表后补全 JD）：

```cmd
python boss_selenium.py --backfill-jd --input "data/processed/quant_intern/jobs_filtered.parquet"
```

说明：
- 该命令会就地更新输入文件的 `job_jd` 字段
- 默认会清理对应日期的 raw 分片与旧分析文件；若仅测试可加 `--no-cleanup-legacy`
- 同样采用固定标签页复用策略，减少窗口抢焦点

## 6. 分批运行模板（推荐）

若关键词较多，建议分两批跑：

### 批次 A

```cmd
python boss_selenium.py --visible --keywords "量化实习,量化研究员实习,量化研究实习,量化开发实习,量化交易实习,量化策略实习,因子研究实习,Alpha研究实习" --max-pages 3 --max-cards 0 --output-dir "data/raw/boss_jobs"
```

### 批次 B

```cmd
python boss_selenium.py --visible --keywords "资产管理实习,投资实习,金融工程实习,风控建模实习,私募实习,对冲基金实习,证券研究实习" --max-pages 3 --max-cards 0 --output-dir "data/raw/boss_jobs"
```

## 7. 结果快速检查

### 7.1 查看当日文件

```cmd
dir data\raw\boss_jobs\dt=%date:~0,4%-%date:~5,2%-%date:~8,2%
```

（PowerShell 可直接进入对应 `dt=YYYY-MM-DD` 目录查看）

### 7.2 Python 查看每个文件条数

```python
from pathlib import Path
import pandas as pd

p = Path("data/raw/boss_jobs/dt=2026-04-14")
for f in sorted(p.glob("*.parquet")):
    df = pd.read_parquet(f)
    print(f.name, len(df))
```

## 8. 常见问题

- 页面出现安全验证：在浏览器中完成验证后继续
- 某关键词结果为 0：属正常情况，不一定有匹配岗位
- 文件名中文显示乱码：终端编码问题，不影响 Parquet 内容本身
- 补抓 JD 时偶发窗口抢焦点：当前已改为固定标签页复用。若仍偶发，通常发生在浏览器重启瞬间，可适当调大 `restart_every` 降低重启频次

## 9. 推荐默认配置（可长期沿用）

- `--visible`
- `--max-pages 3`
- `--max-cards 0`
- 分批关键词运行

如遇运行时间过长，可改为 `--max-pages 2` 做平衡。  
