#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Boss直聘 针对性关键词搜索抓取（上海 + 在校生/实习 + Parquet）。
使用 DrissionPage 控制浏览器，不依赖 chromedriver，天然绕过 cdc_ 检测。
"""
from __future__ import annotations

import argparse
import datetime
import logging
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus

import pandas as pd
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.errors import ElementNotFoundError

from parquet_sink import write_jobs_to_parquet


def _configure_stdio_encoding() -> None:
    """Windows 控制台常为 GBK；Boss 列表用图标字体会混入 Unicode 私用区字符，导致 print / StreamHandler 崩溃。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            if stream is not None and hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_configure_stdio_encoding()

USE_HEADLESS = os.environ.get("BOSS_SCRAPER_HEADLESS", "0") == "1"
WAIT_TIMEOUT = int(os.environ.get("BOSS_SCRAPER_WAIT", "25"))
RESTART_EVERY = int(os.environ.get("BOSS_SCRAPER_RESTART_EVERY", "5"))
MAX_PAGES_PER_KEYWORD = int(os.environ.get("BOSS_SCRAPER_MAX_PAGES", "10"))
LIST_WAIT_MAX = int(os.environ.get("BOSS_WAIT_LIST_MAX", "90"))
LIST_POLL_INTERVAL = float(os.environ.get("BOSS_WAIT_LIST_POLL", "5.0") or "5.0")
# 列表页分段下拉：最多轮数；见到「评价」型嵌入区块达到 2 个即停（避免只滚到第一个评价栏就停）
LIST_SCROLL_MAX_ROUNDS = int(os.environ.get("BOSS_LIST_SCROLL_MAX", "40") or "40")
LIST_SCROLL_PIXEL = int(os.environ.get("BOSS_LIST_SCROLL_PIXEL", "480") or "480")

# ---------- 请求节奏 ----------


def _sleep_pair(name: str, lo: float, hi: float) -> tuple[float, float]:
    raw = (os.environ.get(name) or "").strip()
    if raw:
        parts = raw.replace("，", ",").split(",", 1)
        if len(parts) == 2:
            try:
                a, b = float(parts[0].strip()), float(parts[1].strip())
                return (min(a, b), max(a, b))
            except ValueError:
                pass
    return (lo, hi)


SLEEP_MULT = float(os.environ.get("BOSS_SLEEP_MULT", "1") or "1")
SLEEP_AFTER_NAV = _sleep_pair("BOSS_SLEEP_AFTER_NAV", 8.0, 18.0)
SLEEP_AFTER_SHELL = _sleep_pair("BOSS_SLEEP_AFTER_SHELL", 3.0, 8.0)
SLEEP_SCROLL = _sleep_pair("BOSS_SLEEP_SCROLL", 4.0, 10.0)
SLEEP_BETWEEN_PAGES = _sleep_pair("BOSS_SLEEP_BETWEEN_PAGES", 22.0, 48.0)
SLEEP_BETWEEN_KEYWORDS = _sleep_pair("BOSS_SLEEP_BETWEEN_KEYWORDS", 72.0, 150.0)
SLEEP_AFTER_RESTART = _sleep_pair("BOSS_SLEEP_AFTER_RESTART", 22.0, 45.0)
SLEEP_PER_JOB_ROW = _sleep_pair("BOSS_SLEEP_PER_JOB_ROW", 0.12, 0.45)
SLEEP_EVERY_N_JOBS = max(0, int(os.environ.get("BOSS_SLEEP_EVERY_N_JOBS", "8")))
SLEEP_BATCH_PAUSE = _sleep_pair("BOSS_SLEEP_BATCH_PAUSE", 5.0, 14.0)


def polite_sleep(lo: float, hi: float) -> None:
    a, b = min(lo, hi) * SLEEP_MULT, max(lo, hi) * SLEEP_MULT
    if b <= 0 and a <= 0:
        return
    time.sleep(random.uniform(max(0.0, a), max(a, b)))


# 上海 + 在校生
CITY_SHANGHAI = "101020100"
EXP_STUDENT = "108"
# 新版搜索页为 /web/geek/jobs，旧版为 /web/geek/job；与浏览器地址不一致时列表 DOM 与脚本 URL 可能不对版
_GEEK_SEARCH_SEGMENT = (
    (os.environ.get("BOSS_GEEK_SEARCH_PATH", "jobs") or "jobs").strip().strip("/")
)

SEARCH_TASKS: List[Tuple[str, str]] = [
    ("量化实习", "量化/资管"),
    ("量化研究员实习", "量化/资管"),
    ("量化研究实习", "量化/资管"),
    ("量化开发实习", "量化/资管"),
    ("量化交易实习", "量化/资管"),
    ("量化策略实习", "量化/资管"),
    ("因子研究实习", "量化/资管"),
    ("Alpha研究实习", "量化/资管"),
    ("资产管理实习", "量化/资管"),
    ("投资实习", "量化/资管"),
    ("金融工程实习", "证券/金工"),
    ("风控建模实习", "量化/资管"),
    ("私募实习", "量化/资管"),
    ("对冲基金实习", "量化/资管"),
    ("证券研究实习", "证券/金工"),
]

JOB_CARD_CSS = (
    "css:li.job-card-wrapper, "
    "div.job-card-left, "
    "ul.job-list-box > li, "
    "div.search-job-result li.job-card-wrapper"
)


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("boss_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("boss_scraper")

city_map = {
    "北京": ["北京"], "天津": ["天津"],
    "山西": ["太原", "阳泉", "晋城", "长治", "临汾", "运城", "忻州", "吕梁", "晋中", "大同", "朔州"],
    "河北": ["沧州", "石家庄", "唐山", "保定", "廊坊", "衡水", "邯郸", "邢台", "张家口", "辛集", "秦皇岛", "定州", "承德", "涿州"],
    "山东": ["济南", "淄博", "聊城", "德州", "滨州", "济宁", "菏泽", "枣庄", "烟台", "威海", "泰安", "青岛", "临沂", "莱芜", "东营", "潍坊", "日照"],
    "河南": ["郑州", "新乡", "鹤壁", "安阳", "焦作", "濮阳", "开封", "驻马店", "商丘", "三门峡", "南阳", "洛阳", "周口", "许昌", "信阳", "漯河", "平顶山", "济源"],
    "广东": ["珠海", "中山", "肇庆", "深圳", "清远", "揭阳", "江门", "惠州", "河源", "广州", "佛山", "东莞", "潮州", "汕尾", "梅州", "阳江", "云浮", "韶关", "湛江", "汕头", "茂名"],
    "浙江": ["舟山", "温州", "台州", "绍兴", "衢州", "宁波", "丽水", "金华", "嘉兴", "湖州", "杭州"],
    "宁夏": ["中卫", "银川", "吴忠", "石嘴山", "固原"],
    "江苏": ["镇江", "扬州", "盐城", "徐州", "宿迁", "无锡", "苏州", "南通", "南京", "连云港", "淮安", "常州", "泰州"],
    "湖南": ["长沙", "邵阳", "怀化", "株洲", "张家界", "永州", "益阳", "湘西", "娄底", "衡阳", "郴州", "岳阳", "常德", "湘潭"],
    "吉林": ["长春", "通化", "松原", "四平", "辽源", "吉林", "延边", "白山", "白城"],
    "福建": ["漳州", "厦门", "福州", "三明", "莆田", "宁德", "南平", "龙岩", "泉州"],
    "甘肃": ["张掖", "陇南", "兰州", "嘉峪关", "白银", "武威", "天水", "庆阳", "平凉", "临夏", "酒泉", "金昌", "甘南", "定西"],
    "陕西": ["榆林", "西安", "延安", "咸阳", "渭南", "铜川", "商洛", "汉中", "宝鸡", "安康"],
    "辽宁": ["营口", "铁岭", "沈阳", "盘锦", "辽阳", "锦州", "葫芦岛", "阜新", "抚顺", "丹东", "大连", "朝阳", "本溪", "鞍山"],
    "江西": ["鹰潭", "宜春", "上饶", "萍乡", "南昌", "景德镇", "吉安", "抚州", "新余", "九江", "赣州"],
    "黑龙江": ["伊春", "七台河", "牡丹江", "鸡西", "黑河", "鹤岗", "哈尔滨", "大兴安岭", "绥化", "双鸭山", "齐齐哈尔", "佳木斯", "大庆"],
    "安徽": ["宣城", "铜陵", "六安", "黄山", "淮南", "合肥", "阜阳", "亳州", "安庆", "池州", "宿州", "芜湖", "马鞍山", "淮北", "滁州", "蚌埠"],
    "湖北": ["孝感", "武汉", "十堰", "荆门", "黄冈", "襄阳", "咸宁", "随州", "黄石", "恩施", "鄂州", "荆州", "宜昌", "潜江", "天门", "神农架", "仙桃"],
    "青海": ["西宁", "海西", "海东", "玉树", "黄南", "海南", "海北", "果洛"],
    "新疆": ["乌鲁木齐", "克州", "阿勒泰", "五家渠", "石河子", "伊犁", "吐鲁番", "塔城", "克拉玛依", "喀什", "和田", "哈密", "昌吉", "博尔塔拉", "阿克苏", "巴音郭楞", "阿拉尔", "图木舒克", "铁门关"],
    "贵州": ["铜仁", "黔东南", "贵阳", "安顺", "遵义", "黔西南", "黔南", "六盘水", "毕节"],
    "四川": ["遂宁", "攀枝花", "眉山", "凉山", "成都", "巴中", "广安", "自贡", "甘孜", "资阳", "宜宾", "雅安", "内江", "南充", "绵阳", "泸州", "乐山", "广元", "德阳", "达州", "阿坝"],
    "上海": ["上海"],
    "广西": ["南宁", "贵港", "玉林", "梧州", "钦州", "柳州", "来宾", "贺州", "河池", "桂林", "防城港", "崇左", "北海", "百色"],
    "西藏": ["拉萨", "山南", "日喀则", "那曲", "林芝", "昌都", "阿里"],
    "云南": ["昆明", "红河", "大理", "玉溪", "昭通", "西双版纳", "文山", "曲靖", "普洱", "怒江", "临沧", "丽江", "迪庆", "德宏", "楚雄", "保山"],
    "内蒙古": ["呼和浩特", "乌兰察布", "兴安", "赤峰", "呼伦贝尔", "锡林郭勒", "乌海", "通辽", "巴彦淖尔", "阿拉善", "鄂尔多斯", "包头"],
    "海南": ["海口", "三沙", "三亚", "临高", "五指山", "陵水", "文昌", "万宁", "白沙", "乐东", "澄迈", "屯昌", "定安", "东方", "保亭", "琼中", "琼海", "儋州", "昌江"],
    "重庆": ["重庆"],
}


def build_search_url(query: str, page: int = 1) -> str:
    q = quote_plus(query)
    seg = _GEEK_SEARCH_SEGMENT
    return (
        f"https://www.zhipin.com/web/geek/{seg}?query={q}"
        f"&city={CITY_SHANGHAI}&experience={EXP_STUDENT}&page={page}"
    )


# --------------- 浏览器管理 ---------------

# 复用本机 Chrome 的 cookie：必须指定「User Data 根目录」+「具体 profile 文件夹名」。
# 多 profile 时若只打开 User Data 不指定 profile，Chrome 会先让你选帐号/配置，自动化会卡住。
# 在资源管理器中打开 User Data，可见 Default、Profile 1、Profile 2… 选装了 Boss 且已登录的那个。
_CHROME_USER_DATA_ROOT = (
    os.environ.get("BOSS_CHROME_USER_DATA", "").strip()
    or os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "User Data")
)
_CHROME_PROFILE_DIR = (os.environ.get("BOSS_CHROME_PROFILE", "Default") or "Default").strip()


def init_browser() -> ChromiumPage:
    co = ChromiumOptions()
    if USE_HEADLESS:
        co.headless()
    co.set_argument("--window-size", "1920,1080")
    co.set_argument("--lang", "zh-CN")
    co.set_argument("--no-first-run")
    co.set_argument("--no-default-browser-check")
    co.set_argument("--disable-gpu")

    if os.environ.get("BOSS_FRESH_PROFILE", "0") == "1":
        co.auto_port()
        log.info("使用全新临时配置（无登录状态）")
    elif os.path.isdir(_CHROME_USER_DATA_ROOT):
        co.set_user_data_path(_CHROME_USER_DATA_ROOT)
        co.set_user(_CHROME_PROFILE_DIR)
        log.info(
            "复用 Chrome 配置: user-data-dir=%s | profile-directory=%s（请先关闭所有 Chrome 窗口）",
            _CHROME_USER_DATA_ROOT,
            _CHROME_PROFILE_DIR,
        )
        log.info(
            "若仍出现配置选择页: 把 BOSS_CHROME_PROFILE 改成实际文件夹名，例如 Profile 1"
        )
    else:
        co.auto_port()
        log.warning("未找到 Chrome User Data（%s），改用临时配置", _CHROME_USER_DATA_ROOT)

    page = ChromiumPage(addr_or_opts=co)
    page.set.timeouts(base=WAIT_TIMEOUT, page_load=60)

    log.info("先访问 Boss 主页，建立正常浏览 session…")
    page.get("https://www.zhipin.com/")
    polite_sleep(5.0, 10.0)
    return page


def restart_browser(old: Optional[ChromiumPage]) -> ChromiumPage:
    if old is not None:
        try:
            old.quit()
        except Exception as e:
            log.debug("quit 旧浏览器: %s", e)
        polite_sleep(*SLEEP_AFTER_RESTART)
    return init_browser()


# --------------- 页面状态检测 ---------------

def _is_security_page(page: ChromiumPage) -> bool:
    try:
        title = page.title or ""
    except Exception:
        title = ""
    if "安全验证" in title:
        return True
    try:
        html = page.html or ""
    except Exception:
        return False
    return "page-verify" in html and "geetest" in html


def _page_looks_empty(page: ChromiumPage) -> bool:
    try:
        html = page.html or ""
    except Exception:
        return False
    markers = ("暂无相关职位", "没有更多", "无相关职位", "暂无数据", "职位已过期")
    return any(m in html for m in markers)


def _job_detail_link_count(page: ChromiumPage) -> int:
    """职位列表卡片内通常带 job_detail 详情链接，用作列表已渲染的兜底判断。"""
    try:
        n = page.run_js(
            "return document.querySelectorAll(\"a[href*='job_detail']\").length"
        )
        return int(n) if n is not None else 0
    except Exception:
        return 0


def _card_has_job_detail_link(card) -> bool:
    try:
        return bool(card.ele("css:a[href*='job_detail']", timeout=0.35))
    except Exception:
        return False


def filter_real_job_cards(cards: list) -> list:
    """去掉列表里嵌入的「评价」等非职位卡片（无 job_detail 的 li 常为评价/运营位）。"""
    return [c for c in cards if _card_has_job_detail_link(c)]


def dedupe_job_cards_by_detail_url(cards: list) -> list:
    """
    同一职位卡片内常有多个 li 嵌套，XPath //li[.//a[@job_detail]] 会命中大量重复节点。
    按详情页 URL 去重，只保留每个职位一条（与「点进列表项即该 JD」一致）。
    """
    seen: set[str] = set()
    out: list = []
    for c in cards:
        u = job_detail_url_from_card(c)
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(c)
    return out


def _count_review_like_list_blocks(page: ChromiumPage) -> int:
    """
    统计列表区域内「像评价模块」的块数量（无职位详情链 + 文案命中）。
    用于：继续下拉加载更多职位，直到此类块出现第 2 个时停止下拉。
    """
    js = r"""
(function () {
  const re = /公司评价|职位评价|企业评价|体验评价|我要评价|说说你为什么|推荐朋友|对该公司|面试评价|员工评价/;
  const items = document.querySelectorAll("li,div[class*='card']");
  let n = 0;
  for (const el of items) {
    try {
      if (el.querySelector("a[href*='job_detail']")) continue;
      const t = (el.innerText || "").slice(0, 500);
      if (re.test(t) && t.length < 2000) n++;
    } catch (e) {}
  }
  return n;
})()
"""
    try:
        n = page.run_js(js)
        return int(n) if n is not None else 0
    except Exception:
        return 0


def scroll_job_list_load_more(
    page: ChromiumPage,
    quick: bool = False,
    stop_after_detail_links: int = 0,
) -> None:
    """
    分段向下滚动以触发懒加载；跳过仅滚到第一个「评价」栏就停的问题。
    当页面内检测到的「评价型」嵌入块数量 >= 2 时停止继续下拉（之后仍解析当前 DOM 内全部真实职位卡片）。
    quick=True：少轮、大步；若 stop_after_detail_links>0，职位链接数达到该值即停（避免只抓前 N 条却滚出几十条）。
    """
    max_rounds = min(LIST_SCROLL_MAX_ROUNDS, 5) if quick else LIST_SCROLL_MAX_ROUNDS
    pixel = min(LIST_SCROLL_PIXEL + 200, 900) if quick else LIST_SCROLL_PIXEL
    last_links = _job_detail_link_count(page)
    for i in range(max_rounds):
        links_now = _job_detail_link_count(page)
        if stop_after_detail_links > 0 and links_now >= stop_after_detail_links:
            log.info(
                "已加载约 %d 条 job_detail 链接（目标≥%d），停止继续下拉",
                links_now,
                stop_after_detail_links,
            )
            break
        review_n = _count_review_like_list_blocks(page)
        if review_n >= 2:
            log.info(
                "检测到至少 2 处列表内「评价」型区块（当前 job_detail 链接数≈%d），停止继续下拉",
                links_now,
            )
            break
        try:
            page.scroll.down(pixel)
        except Exception:
            try:
                page.scroll.to_bottom()
            except Exception:
                break
        polite_sleep(1.0, 2.2)
        links = _job_detail_link_count(page)
        if links > last_links:
            last_links = links
        if stop_after_detail_links > 0 and links >= stop_after_detail_links:
            break


def _boss_abs_url(href: str) -> str:
    h = (href or "").strip()
    if not h:
        return ""
    if h.startswith("http://") or h.startswith("https://"):
        return h
    if h.startswith("//"):
        return "https:" + h
    if h.startswith("/"):
        return "https://www.zhipin.com" + h
    return "https://www.zhipin.com/" + h.lstrip("/")


def job_detail_url_from_card(card) -> str:
    """卡片内可能有多条 job_detail 链（公司/职位），取第一条指向职位详情的链接。"""
    try:
        for a in card.eles("css:a[href*='job_detail']", timeout=0.5):
            href = (a.attr("href") or "").strip()
            if "job_detail" in href:
                return _boss_abs_url(href)
    except Exception:
        pass
    return ""


def _strip_private_use_area(s: str) -> str:
    """Boss 列表/详情里薪资等常用图标字体，落在 Unicode 私用区（U+E000–U+F8FF），显示为乱码。"""
    return "".join(c for c in s if not (0xE000 <= ord(c) <= 0xF8FF))


def _decode_boss_pua_digits(s: str) -> str:
    """
    Boss 常把薪资数字渲染到私用区字符（例如 U+E031 => 1）。
    先把可识别的数字映射回阿拉伯数字，再执行后续清洗/解析。
    """
    if not s:
        return s
    mapping = {ord(chr(0xE030 + i)): str(i) for i in range(10)}
    # 线上字体偶发把 "7" 放到 U+E03A（已在真实抓取样本中观察到）。
    mapping[0xE03A] = "7"
    return s.translate(mapping)


def _clean_jd_text(raw: str) -> str:
    """去掉详情页里站点插字、私用区图标字，便于入库与阅读。"""
    if not raw:
        return raw
    s = raw.strip()
    s = re.sub(r"kanzhun", "", s, flags=re.I)
    s = re.sub(r"职直聘位", "职位", s)
    s = re.sub(r"岗位职\s*责", "岗位职责", s)
    s = re.sub(r"职位BOSS直聘", "职位", s)
    s = re.sub(r"(BOSS直聘){2,}", "BOSS直聘", s)
    s = re.sub(r"直聘岗位职责", "岗位职责", s)
    # 正文中被拆入的「直聘」二字（非品牌语「BOSS直聘」整体）
    for bad, good in (
        ("金融市直聘场", "金融市场"),
        ("实习直聘生", "实习生"),
        ("应届直聘生", "应届生"),
        ("薪资直聘范围", "薪资范围"),
        ("工作直聘职责", "工作职责"),
        ("岗位职boss责", "岗位职责"),
        ("职boss责", "职责"),
        ("协助开发boss", "协助开发"),
        ("机直聘器", "机器"),
        ("基于boss", "基于"),
    ):
        s = s.replace(bad, good)
    s = re.sub(r"(?<=[\u4e00-\u9fff])boss(?=[\u4e00-\u9fff])", "", s, flags=re.I)
    s = _strip_private_use_area(s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _extract_jd_from_tab(tab) -> str:
    jd = (
        _ele_text(tab, "css:.job-sec-text")
        or _ele_text(tab, "css:div.job-sec-text")
        or _ele_text(tab, "css:.job-detail-box .text")
        or _ele_text(tab, "css:div.job-detail")
    )
    if not jd:
        try:
            h = tab.html or ""
            jd = h[:30000] if h else ""
        except Exception:
            jd = ""
    jd_raw = (jd or "").strip()[:80000]
    extra = ""
    try:
        sal = (
            _ele_text(tab, "css:.job-info-primary .salary", "")
            or _ele_text(tab, "css:.job-info .salary", "")
            or _ele_text(tab, "css:.job-banner .salary", "")
            or _ele_text(tab, "css:span.salary", "")
        )
        sal = _strip_private_use_area((sal or "").strip())
        if sal and re.search(r"[0-9０-９零一二三四五六七八九十两]", sal):
            extra = "【薪资】" + sal + "\n\n"
        elif sal and ("元" in sal or "薪" in sal or "K" in sal or "k" in sal):
            extra = "【薪资】" + sal + "\n\n"
    except Exception:
        pass
    return _clean_jd_text((extra + jd_raw).strip()[:80000])


def fetch_jd_reuse_tab(tab, detail_url: str) -> str:
    """复用同一标签页抓取 JD，避免频繁创建/关闭标签页导致抢焦点。"""
    if not detail_url:
        return ""
    try:
        tab.get(detail_url)
        tab.wait.doc_loaded()
        polite_sleep(1.2, 2.5)
        return _extract_jd_from_tab(tab)
    except Exception as e:
        log.debug("抓取 JD 失败: %s", e)
        return ""


def fetch_job_jd_in_new_tab(
    page: ChromiumPage, detail_url: str, list_tab_id: str
) -> str:
    """在新标签打开详情页抓取 JD，再关闭并切回列表页，避免 SPA 后退不稳定。"""
    if not detail_url:
        return ""
    sub = None
    try:
        sub = page.new_tab(detail_url, background=True)
        sub.wait.doc_loaded()
        polite_sleep(1.2, 2.5)
        return _extract_jd_from_tab(sub)
    except Exception as e:
        log.debug("抓取 JD 失败: %s", e)
        return ""
    finally:
        if sub is not None:
            try:
                sub.close()
            except Exception:
                pass
        try:
            page.activate_tab(list_tab_id)
        except Exception:
            pass


def _page_still_loading_shell(page: ChromiumPage) -> bool:
    """SPA 壳页常见「加载中，请稍候」，此时 DOM 里还没有职位卡片。"""
    try:
        html = page.html or ""
    except Exception:
        return False
    if "加载中" not in html and "请稍候" not in html:
        return False
    return _job_detail_link_count(page) < 2


def _find_job_cards(page: ChromiumPage) -> list:
    """查找职位卡片；/geek/jobs 与 /geek/job 的 class 可能不同，多路选择器 + job_detail 兜底。"""
    selectors: List[str] = [
        "css:li.job-card-box",
        "css:div.job-card-wrap li.job-card-box",
        "css:li.job-card-wrapper",
        "css:div.job-card-left",
        "css:ul.job-list-box > li",
        "css:div.search-job-result li.job-card-wrapper",
        "xpath://li[contains(@class,'job-card-wrapper')]",
        "xpath://li[contains(@class,'job-card') and contains(@class,'item')]",
        "css:li[class*='job-card-wrapper']",
        "css:li[class*='job-card'][class*='box']",
        "css:[class*='job-card-wrap'] li",
        # 下列易把卡片内每个 li 都当成一条，仅当命中数较少时才采用（否则靠外层选择器 + URL 去重）
        "xpath://li[.//a[contains(@href,'job_detail')]]",
        "xpath://div[contains(@class,'job-card')][.//a[contains(@href,'job_detail')]]",
    ]
    for selector in selectors:
        try:
            els = page.eles(selector, timeout=2.5)
            if not els:
                continue
            if "job_detail" in selector and len(els) > 28:
                continue
            return list(els)
        except Exception:
            continue
    return []


def resolve_security_challenge(page: ChromiumPage) -> bool:
    if not _is_security_page(page):
        return True
    log.warning("当前页面为 BOSS「安全验证」。脚本已完全停下，不会操作浏览器。")
    if USE_HEADLESS:
        log.error("无头模式无法通过安全验证，请勿使用 --headless")
        return False
    print(
        "\n"
        "====================================================\n"
        "  请在浏览器窗口中完成安全验证（点击按钮 / 滑块）。\n"
        "  Boss 会自动跳转回搜索结果页。\n"
        "  确认看到【职位列表】后，回到此终端按【回车】继续。\n"
        "====================================================\n"
    )
    try:
        input()
    except EOFError:
        return False
    polite_sleep(3.0, 6.0)

    if _is_security_page(page):
        log.error("按回车后页面仍为安全验证，请在浏览器中重新操作后再按回车")
        print("\n>>> 仍在验证页，请重新操作后按【回车】 <<<\n")
        try:
            input()
        except EOFError:
            return False
        polite_sleep(2.0, 4.0)

    if _is_security_page(page):
        log.error("二次确认后仍为安全验证页，中止")
        return False
    log.info("安全验证已通过，继续抓取")
    return True


def wait_for_job_list(page: ChromiumPage) -> bool:
    """
    等待 SPA 渲染出职位列表（或空结果 / 安全验证页）。
    返回 False 表示浏览器不可用。
    """
    deadline = time.time() + LIST_WAIT_MAX
    last_log = 0.0
    poll = max(LIST_POLL_INTERVAL, 5.0)

    while time.time() < deadline:
        try:
            title = page.title or ""
        except Exception:
            log.error("读取 title 异常，浏览器可能已崩溃")
            return False

        if "安全验证" in title:
            log.info("检测到安全验证页，结束列表等待")
            return True

        if _page_still_loading_shell(page):
            time.sleep(min(3.0, poll))
            continue

        cards = _find_job_cards(page)
        if cards:
            log.info("职位列表已挂载，共 %d 个卡片节点", len(cards))
            polite_sleep(*SLEEP_AFTER_SHELL)
            return True

        if _page_looks_empty(page):
            log.info("检测到空结果提示，列表等待结束")
            polite_sleep(*SLEEP_AFTER_SHELL)
            return True

        now = time.time()
        if now - last_log >= 15.0:
            try:
                u = page.url or ""
            except Exception:
                u = ""
            log.info(
                "仍在等待职位列表渲染… 已等待 %.0fs / 上限 %ds | "
                "job_detail 链接数≈%d | url=%s",
                now - (deadline - LIST_WAIT_MAX),
                LIST_WAIT_MAX,
                _job_detail_link_count(page),
                u[:160],
            )
            last_log = now
        time.sleep(poll)

    log.warning("等待 %ds 仍未出现职位列表节点", LIST_WAIT_MAX)
    polite_sleep(*SLEEP_AFTER_SHELL)
    return True


# --------------- 解析职位卡片 ---------------

def _ele_text(parent, locator: str, default: str = "") -> str:
    """在父元素内查找子元素并返回其文本，找不到则返回 default。"""
    try:
        el = parent.ele(locator, timeout=0.5)
        return (el.text or "").strip() if el else default
    except (ElementNotFoundError, Exception):
        return default


def _ele_texts(parent, locator: str) -> list[str]:
    try:
        els = parent.eles(locator, timeout=0.5)
        return [(e.text or "").strip() for e in els if (e.text or "").strip()]
    except Exception:
        return []


def parse_salary_text(raw: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """从 Boss 薪资文案中尽力解析数值区间与单位；无法解析时返回 (None, None, None)。"""
    if not raw:
        return None, None, None
    s = _decode_boss_pua_digits((raw or "").strip())
    s = _strip_private_use_area(s)
    if not s or s in ("面议", "薪资面议", "无", "无数据"):
        return None, None, None
    fw_digits = "０１２３４５６７８９－"
    fw_ascii = "0123456789-"
    s = s.translate(str.maketrans(fw_digits, fw_ascii))

    if "元/天" in s or "元每天" in s or ("/天" in s and "元" in s):
        nums = re.findall(r"(\d+(?:\.\d+)?)", s)
        if len(nums) >= 2:
            return float(nums[0]), float(nums[1]), "yuan_per_day"
        if len(nums) == 1:
            v = float(nums[0])
            return v, v, "yuan_per_day"
        return None, None, None

    if "万" in s and re.search(r"\d", s):
        nums = re.findall(r"(\d+(?:\.\d+)?)", s)
        if len(nums) >= 2:
            return float(nums[0]) * 10000, float(nums[1]) * 10000, "yuan_per_month"
        if len(nums) == 1:
            v = float(nums[0]) * 10000
            return v, v, "yuan_per_month"

    if re.search(r"[Kk千]", s):
        nums = re.findall(r"(\d+(?:\.\d+)?)", s)
        if len(nums) >= 2:
            return float(nums[0]) * 1000, float(nums[1]) * 1000, "yuan_per_month"
        if len(nums) == 1:
            v = float(nums[0]) * 1000
            return v, v, "yuan_per_month"

    return None, None, None


def parse_job_card(
    card, category_label: str, keyword: str, crawl_date: str
) -> Optional[Dict[str, Any]]:
    if not _card_has_job_detail_link(card):
        return None
    job_title = (
        _ele_text(card, "css:a.job-name")
        or _ele_text(card, "css:div.job-title a.job-name")
        or _ele_text(card, "css:div.job-info a.job-name")
        or _ele_text(card, "css:span.job-name")
    )
    if not job_title:
        return None

    job_location = (
        _ele_text(card, "css:span.company-location")
        or _ele_text(card, "css:span.job-area")
        or _ele_text(card, "css:span.job-area-wrapper")
    )
    job_company = (
        _ele_text(card, "css:span.boss-name")
        or _ele_text(card, "css:a.boss-info span.boss-name")
        or _ele_text(card, "css:h3.company-name a")
        or _ele_text(card, "css:span.company-name")
        or _ele_text(card, "css:div.company-info a")
    )

    company_tags = _ele_texts(card, "css:ul.company-tag-list li")
    job_industry = company_tags[0] if len(company_tags) > 0 else "无"
    job_finance = company_tags[1] if len(company_tags) > 1 else "无"
    job_scale = company_tags[2] if len(company_tags) > 2 else "无"

    job_welfare = (
        _ele_text(card, "css:div.job-welfare")
        or _ele_text(card, "css:div.info-desc")
        or _ele_text(card, "css:div.job-card-footer.clearfix div.info-desc")
        or _ele_text(card, "css:div.job-card-footer span")
        or "无"
    )

    salary_text = (
        _ele_text(card, "css:span.job-salary")
        or _ele_text(card, "css:div.job-title span.job-salary")
        or _ele_text(card, "css:span.salary")
    )
    salary_text = _decode_boss_pua_digits(salary_text)
    salary_min, salary_max, salary_unit = parse_salary_text(salary_text)

    tag_list = _ele_texts(card, "css:ul.tag-list li")
    edu_re = re.compile(r"(学历不限|本科|硕士|博士|大专|中专|高中)")
    if len(tag_list) >= 3:
        # 新版卡片结构常见：第1项=出勤，第2项=时长，第3项=学历
        job_experience = ",".join(tag_list[:2])
        job_education = tag_list[2]
        job_skills = ",".join(tag_list[3:]) if len(tag_list) > 3 else "无"
    elif len(tag_list) == 2:
        if edu_re.search(tag_list[1]):
            job_experience = tag_list[0]
            job_education = tag_list[1]
        else:
            job_experience = ",".join(tag_list)
            job_education = "无"
        job_skills = "无"
    else:
        job_experience = tag_list[0] if tag_list else "无"
        job_education = "无"
        job_skills = "无"
    job_tags = ",".join(tag_list) if tag_list else "无"

    province = province_for_location(job_location)
    city = job_location.split("·")[0].strip() if job_location else ""

    company_url = ""
    try:
        cel = (
            card.ele("css:a.boss-info", timeout=0.35)
            or card.ele("css:h3.company-name a", timeout=0.35)
        )
        if cel:
            company_url = _boss_abs_url((cel.attr("href") or "").strip())
    except Exception:
        pass

    publish_text = (
        _ele_text(card, "css:span.job-pub-time")
        or _ele_text(card, "css:span.time")
        or _ele_text(card, "css:span.job-time")
        or _ele_text(card, "css:.job-time")
        or ""
    )

    detail_url = job_detail_url_from_card(card)
    crawl_time = datetime.datetime.now().replace(microsecond=0).isoformat()

    return {
        "source": "boss",
        "category": category_label,
        "keyword": keyword,
        "city": city,
        "job_title": job_title,
        "province": province,
        "job_location": job_location,
        "job_company": job_company,
        "job_industry": job_industry,
        "job_finance": job_finance,
        "job_scale": job_scale,
        "job_welfare": job_welfare,
        "salary_text": salary_text,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_unit": salary_unit,
        "job_experience": job_experience,
        "job_education": job_education,
        "job_skills": job_skills,
        "job_tags": job_tags,
        "job_jd": "",
        "detail_url": detail_url,
        "company_url": company_url,
        "publish_text": publish_text,
        "crawl_time": crawl_time,
        "crawl_date": crawl_date,
    }


def province_for_location(job_location: str) -> str:
    city = job_location.split("·")[0].strip() if job_location else ""
    for p, cities in city_map.items():
        if city in cities:
            return p
    return ""


def resolve_search_tasks(keywords_arg: Optional[str]) -> List[Tuple[str, str]]:
    if keywords_arg:
        parts = [p.strip() for p in keywords_arg.split(",") if p.strip()]
        return [(p, "自定义") for p in parts]
    return list(SEARCH_TASKS)


def row_dedupe_key(row: Dict[str, Any]) -> tuple[Any, ...]:
    u = (row.get("detail_url") or "").strip()
    if u:
        return ("url", u)
    return (
        "legacy",
        row["job_title"],
        row["job_company"],
        row["job_location"],
    )


def _save_debug_page_html(output_dir: str, keyword: str, page_no: int, html: str) -> None:
    """保存每页渲染后的 HTML，便于定位站点 DOM 变更。"""
    safe_kw = re.sub(r'[<>:"/\\|?*]+', "_", (keyword or "").strip()) or "kw"
    debug_dir = Path(output_dir) / "_debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    p = debug_dir / f"{safe_kw}_p{page_no}.html"
    try:
        p.write_text(html or "", encoding="utf-8")
        log.info("已保存调试 HTML: %s", p)
    except OSError as e:
        log.warning("保存调试 HTML 失败: %s", e)


# --------------- 主流程 ---------------

def run_scrape(
    dry_run: bool,
    tasks: Sequence[Tuple[str, str]],
    max_pages: int,
    fetch_jd: bool = False,
    max_jd: int = 0,
    max_cards: int = 0,
    output_dir: str = "data/raw/boss_jobs",
) -> None:
    today = datetime.date.today().strftime("%Y-%m-%d")
    sink_parquet = not dry_run
    if dry_run:
        log.info("dry-run：不写 Parquet")
    else:
        log.info("写入 Parquet：%s", output_dir)

    seen: set[tuple[Any, ...]] = set()
    page: Optional[ChromiumPage] = None
    task_list = list(tasks)
    try:
        page = init_browser()
        jd_tab = page.new_tab("about:blank", background=True) if fetch_jd else None
        for kw_idx, (keyword, category_label) in enumerate(task_list):
            if kw_idx > 0 and kw_idx % RESTART_EVERY == 0:
                log.info("已处理 %d 个关键词，重启浏览器…", kw_idx)
                page = restart_browser(page)
                jd_tab = page.new_tab("about:blank", background=True) if fetch_jd else None

            log.info("%s 关键词 [%s] 分组=%s", today, keyword, category_label)
            pg = 1
            prev_first_title: Optional[str] = None

            while pg <= max_pages:
                url = build_search_url(keyword, pg)
                log.info("GET %s", url)
                try:
                    page.get(url)
                except Exception as e:
                    log.warning("page.get() 异常 (%s)，重启浏览器…", e)
                    page = restart_browser(page)
                    page.get(url)
                polite_sleep(*SLEEP_AFTER_NAV)

                if not wait_for_job_list(page):
                    log.warning("浏览器不可用，重启后重试…")
                    page = restart_browser(page)
                    continue

                if not resolve_security_challenge(page):
                    log.error("安全验证未通过，中止抓取")
                    return

                list_tab_id = page.tab_id

                cards = _find_job_cards(page)
                if not cards:
                    wait_for_job_list(page)
                    cards = _find_job_cards(page)

                quick_scroll = bool(max_cards and max_cards <= 25)
                stop_links = (max_cards + 12) if quick_scroll and max_cards > 0 else 0
                scroll_job_list_load_more(
                    page, quick=quick_scroll, stop_after_detail_links=stop_links
                )

                cards = _find_job_cards(page)
                job_cards = filter_real_job_cards(cards)
                job_cards = dedupe_job_cards_by_detail_url(job_cards)
                _save_debug_page_html(output_dir, keyword, pg, page.html or "")

                if not cards:
                    try:
                        with open("boss_last_page.html", "w", encoding="utf-8") as f:
                            f.write(page.html)
                        log.warning("关键词 %s 第 %d 页无列表，已保存 boss_last_page.html", keyword, pg)
                    except OSError as e:
                        log.debug("保存调试 HTML 失败: %s", e)
                    log.info("关键词 %s 第 %d 页无列表，结束分页", keyword, pg)
                    break

                if not job_cards:
                    log.warning(
                        "关键词 %s 第 %d 页 原始节点 %d 条，过滤后无带 job_detail 的职位卡片",
                        keyword,
                        pg,
                        len(cards),
                    )
                    break

                raw_n = len(job_cards)
                if max_cards > 0:
                    job_cards = job_cards[:max_cards]
                    log.info(
                        "本页仅处理前 %d 条真实职位（过滤后共 %d 条，已截断）",
                        len(job_cards),
                        raw_n,
                    )
                if fetch_jd and max_jd > 0:
                    log.info("本页将最多抓取 %d 条 JD（复用固定标签页）", max_jd)

                first_row = parse_job_card(job_cards[0], category_label, keyword, today)
                first_title = first_row["job_title"] if first_row else None
                if first_title and prev_first_title and first_title == prev_first_title:
                    log.info("关键词 %s 第 %d 页与上页首条重复，停止分页", keyword, pg)
                    break
                prev_first_title = first_title

                parsed_page = 0
                jd_done = 0
                page_rows: list[Dict[str, Any]] = []
                for j_i, card in enumerate(job_cards):
                    if SLEEP_EVERY_N_JOBS > 0 and j_i > 0 and j_i % SLEEP_EVERY_N_JOBS == 0:
                        log.debug("已解析 %d 条卡片，批量停顿", j_i)
                        polite_sleep(*SLEEP_BATCH_PAUSE)
                    polite_sleep(*SLEEP_PER_JOB_ROW)
                    row = parse_job_card(card, category_label, keyword, today)
                    if row is None:
                        continue
                    if fetch_jd and max_jd > 0 and jd_done < max_jd:
                        u = job_detail_url_from_card(card)
                        if u:
                            if jd_tab is None:
                                jd_tab = page.new_tab("about:blank", background=True)
                            jd_text = fetch_jd_reuse_tab(jd_tab, u)
                            jd_done += 1
                            row["job_jd"] = jd_text
                            if dry_run and jd_text:
                                log.info(
                                    "DRY [JD] %s | %d 字 | 预览: %s",
                                    row["job_title"],
                                    len(jd_text),
                                    jd_text[:180].replace("\r", " ").replace("\n", " "),
                                )
                    dk = row_dedupe_key(row)
                    if dk in seen:
                        continue
                    seen.add(dk)
                    parsed_page += 1
                    page_rows.append(row)
                    if dry_run:
                        log.info(
                            "DRY [行] %s | %s | %s | %s",
                            row["job_title"],
                            row["job_location"],
                            row["job_company"],
                            row["salary_text"],
                        )
                    print(
                        category_label,
                        keyword,
                        row["job_title"],
                        row["province"],
                        row["job_location"],
                        row["job_company"],
                        row["job_industry"],
                        row["job_finance"],
                        row["job_scale"],
                        row["job_welfare"],
                        row["salary_text"],
                        row["job_experience"],
                        row["job_education"],
                        row["job_skills"],
                        row["job_jd"],
                        sep=" | ",
                    )

                if sink_parquet and page_rows:
                    out = write_jobs_to_parquet(
                        page_rows, output_dir, today, f"{keyword}_p{pg}"
                    )
                    if out is not None:
                        log.info(
                            "Parquet 已写入: %s（本页 %d 条）",
                            out,
                            len(page_rows),
                        )

                log.info(
                    "关键词 %s 第 %d 页 解析 %d 条（累计去重后总条数 %d）",
                    keyword, pg, parsed_page, len(seen),
                )
                if parsed_page == 0:
                    break
                pg += 1
                polite_sleep(*SLEEP_BETWEEN_PAGES)

            if kw_idx < len(task_list) - 1:
                log.info(
                    "关键词 [%s] 完成，进入下一关键词前随机等待 %s~%s 秒（×mult=%s）",
                    keyword,
                    round(SLEEP_BETWEEN_KEYWORDS[0] * SLEEP_MULT, 1),
                    round(SLEEP_BETWEEN_KEYWORDS[1] * SLEEP_MULT, 1),
                    SLEEP_MULT,
                )
                polite_sleep(*SLEEP_BETWEEN_KEYWORDS)

    finally:
        if page is not None:
            try:
                page.quit()
            except Exception as e:
                log.debug("page.quit: %s", e)


def _save_backfill_checkpoint(df: pd.DataFrame, output_path: Path) -> None:
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(output_path)


def cleanup_legacy_outputs(input_path: Path, crawl_dates: list[str]) -> None:
    old_outputs = [
        Path("data/processed/quant_intern/tech_stack_summary.parquet"),
        Path("data/processed/quant_intern/salary_overview.parquet"),
        Path("data/processed/quant_intern/market_report.md"),
    ]
    removed = 0
    for p in old_outputs:
        if p.exists():
            p.unlink()
            removed += 1
            log.info("已删除旧分析文件: %s", p)

    raw_root = Path("data/raw/boss_jobs")
    for d in sorted(set([str(x).strip() for x in crawl_dates if str(x).strip()])):
        part = raw_root / f"dt={d}"
        if part.exists() and part.is_dir():
            files = list(part.glob("*.parquet"))
            for f in files:
                f.unlink()
                removed += 1
            try:
                part.rmdir()
                log.info("已删除原始分区目录: %s", part)
            except OSError:
                log.info("原始分区目录非空，已删除 parquet 分片: %s", part)
    log.info("旧文件清理完成，删除 %d 个文件", removed)


def backfill_jd(
    input_path: str,
    restart_every: int = 120,
    checkpoint_every: int = 20,
    cleanup_legacy: bool = True,
) -> None:
    src = Path(input_path)
    if not src.exists():
        raise FileNotFoundError(f"输入文件不存在: {src}")
    df = pd.read_parquet(src)
    if "detail_url" not in df.columns:
        raise ValueError("输入 parquet 缺少 detail_url 列，无法补抓 JD")
    if "job_jd" not in df.columns:
        df["job_jd"] = ""

    detail = df["detail_url"].fillna("").astype(str).str.strip()
    jd = df["job_jd"].fillna("").astype(str).str.strip()
    todo_idx = df.index[(detail != "") & (jd == "")].tolist()
    total = len(todo_idx)
    log.info("JD 补抓总任务数: %d（文件总行数 %d）", total, len(df))
    if total == 0:
        log.info("没有需要补抓的 JD，跳过抓取。")
        if cleanup_legacy:
            crawl_dates = (
                df["crawl_date"].fillna("").astype(str).tolist()
                if "crawl_date" in df.columns
                else []
            )
            cleanup_legacy_outputs(src, crawl_dates)
        return

    page: Optional[ChromiumPage] = None
    jd_tab = None
    done = 0
    try:
        page = init_browser()
        jd_tab = page.new_tab("about:blank", background=True)
        for i, idx in enumerate(todo_idx, start=1):
            if i > 1 and restart_every > 0 and (i - 1) % restart_every == 0:
                log.info("JD 补抓已处理 %d 条，重启浏览器", i - 1)
                page = restart_browser(page)
                jd_tab = page.new_tab("about:blank", background=True)
            url = str(df.at[idx, "detail_url"]).strip()
            text = fetch_jd_reuse_tab(jd_tab, url)
            if text:
                df.at[idx, "job_jd"] = text
            done += 1

            if done % checkpoint_every == 0:
                _save_backfill_checkpoint(df, src)
                log.info(
                    "JD 补抓进度: %d/%d（最近一条 %d 字）",
                    done,
                    total,
                    len(text),
                )
            polite_sleep(0.8, 1.8)
    finally:
        _save_backfill_checkpoint(df, src)
        if page is not None:
            try:
                page.quit()
            except Exception as e:
                log.debug("page.quit: %s", e)

    still_empty = int(df["job_jd"].fillna("").astype(str).str.strip().eq("").sum())
    log.info("JD 补抓结束，文件已更新: %s", src)
    log.info("补抓后仍为空的 job_jd 条数: %d", still_empty)
    if cleanup_legacy:
        crawl_dates = (
            df["crawl_date"].fillna("").astype(str).tolist()
            if "crawl_date" in df.columns
            else []
        )
        cleanup_legacy_outputs(src, crawl_dates)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Boss直聘 上海在校生/实习 针对性关键词搜索抓取（DrissionPage）"
    )
    parser.add_argument("--dry-run", action="store_true", help="不写 Parquet，仅打印/日志")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw/boss_jobs",
        help="Parquet 输出根目录（会写入 dt=YYYY-MM-DD 子目录）",
    )
    parser.add_argument(
        "--keywords", type=str, default="",
        help="逗号分隔关键词，覆盖默认 SEARCH_TASKS；例：量化实习,数据分析实习",
    )
    disp = parser.add_mutually_exclusive_group()
    disp.add_argument("--visible", action="store_true", help="强制有界面浏览器")
    disp.add_argument("--headless", action="store_true", help="强制无头模式（易触发安全验证）")
    parser.add_argument(
        "--max-pages", type=int, default=0,
        help="每个关键词最多翻页数，0 表示使用 BOSS_SCRAPER_MAX_PAGES（默认 10）",
    )
    parser.add_argument(
        "--fetch-jd",
        action="store_true",
        help="新标签打开详情页抓取 JD，写入 job_jd 列",
    )
    parser.add_argument(
        "--max-jd",
        type=int,
        default=0,
        help="每页最多抓几条详情 JD；与 --fetch-jd 合用。默认 0：若同时指定了 --max-cards 则与其相同，否则为 5",
    )
    parser.add_argument(
        "--max-cards",
        type=int,
        default=0,
        help="每页最多处理几条真实职位（过滤评价块后截断）。例：测试前 15 条 + JD 用 --max-cards 15 --fetch-jd",
    )
    parser.add_argument(
        "--backfill-jd",
        action="store_true",
        help="读取已有 parquet 的 detail_url，逐条补抓 JD 并就地更新 job_jd",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/processed/quant_intern/jobs_filtered.parquet",
        help="--backfill-jd 模式的输入 parquet 路径",
    )
    parser.add_argument(
        "--no-cleanup-legacy",
        action="store_true",
        help="--backfill-jd 完成后不删除 raw 分片和旧分析文件",
    )
    args = parser.parse_args()
    global USE_HEADLESS
    if args.headless:
        USE_HEADLESS = True
    elif args.visible:
        USE_HEADLESS = False
    else:
        USE_HEADLESS = os.environ.get("BOSS_SCRAPER_HEADLESS", "0") == "1"

    tasks = resolve_search_tasks(args.keywords.strip() or None)
    max_pages = args.max_pages if args.max_pages > 0 else MAX_PAGES_PER_KEYWORD
    log.info(
        "headless=%s wait=%ds restart_every=%d max_pages=%d dry_run=%s "
        "output_dir=%s tasks=%d",
        USE_HEADLESS,
        WAIT_TIMEOUT,
        RESTART_EVERY,
        max_pages,
        args.dry_run,
        args.output_dir,
        len(tasks),
    )
    log.info(
        "间隔(秒,×mult=%s): after_nav=%s after_shell=%s scroll=%s "
        "between_pages=%s between_keywords=%s after_restart=%s "
        "per_row=%s every_n=%s batch_pause=%s",
        SLEEP_MULT, SLEEP_AFTER_NAV, SLEEP_AFTER_SHELL, SLEEP_SCROLL,
        SLEEP_BETWEEN_PAGES, SLEEP_BETWEEN_KEYWORDS, SLEEP_AFTER_RESTART,
        SLEEP_PER_JOB_ROW, SLEEP_EVERY_N_JOBS, SLEEP_BATCH_PAUSE,
    )
    max_jd = max(0, args.max_jd)
    max_cards = max(0, args.max_cards)
    if args.backfill_jd:
        backfill_jd(
            input_path=args.input.strip() or "data/processed/quant_intern/jobs_filtered.parquet",
            restart_every=120,
            checkpoint_every=20,
            cleanup_legacy=not args.no_cleanup_legacy,
        )
        return
    if args.fetch_jd and max_jd == 0:
        max_jd = max_cards if max_cards > 0 else 5
    run_scrape(
        dry_run=args.dry_run,
        tasks=tasks,
        max_pages=max_pages,
        fetch_jd=args.fetch_jd,
        max_jd=max_jd,
        max_cards=max_cards,
        output_dir=args.output_dir.strip() or "data/raw/boss_jobs",
    )


if __name__ == "__main__":
    main()
