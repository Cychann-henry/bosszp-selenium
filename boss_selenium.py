#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Boss直聘 针对性关键词搜索抓取（上海 + 在校生/实习 + PostgreSQL）。"""
from __future__ import annotations

import argparse
import datetime
import logging
import os
import random
import sys
import time
from typing import Any, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus

import undetected_chromedriver as uc
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from dbutils import DBUtils

uc.Chrome.__del__ = lambda self: None  # type: ignore[misc, assignment]

USE_HEADLESS = os.environ.get("BOSS_SCRAPER_HEADLESS", "1") == "1"
WAIT_TIMEOUT = int(os.environ.get("BOSS_SCRAPER_WAIT", "25"))
# 每处理多少个关键词重启浏览器（防封 / 句柄）
RESTART_EVERY = int(os.environ.get("BOSS_SCRAPER_RESTART_EVERY", "5"))
MAX_PAGES_PER_KEYWORD = int(os.environ.get("BOSS_SCRAPER_MAX_PAGES", "10"))

# ---------- 请求节奏（防短时间大量访问触发风控）----------
# 均为「秒」；每项为 min,max 随机区间。可用环境变量覆盖，格式：8,18 或 8，18
# BOSS_SLEEP_MULT：整体乘数，例如 1.5 表示所有间隔放大 1.5 倍


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
# 解析列表每条卡片之间微小停顿；每 N 条再额外歇一轮（模拟浏览）
SLEEP_PER_JOB_ROW = _sleep_pair("BOSS_SLEEP_PER_JOB_ROW", 0.12, 0.45)
SLEEP_EVERY_N_JOBS = max(0, int(os.environ.get("BOSS_SLEEP_EVERY_N_JOBS", "8")))
SLEEP_BATCH_PAUSE = _sleep_pair("BOSS_SLEEP_BATCH_PAUSE", 5.0, 14.0)


def polite_sleep(lo: float, hi: float) -> None:
    a, b = min(lo, hi) * SLEEP_MULT, max(lo, hi) * SLEEP_MULT
    if b <= 0 and a <= 0:
        return
    time.sleep(random.uniform(max(0.0, a), max(a, b)))


_REF_CHROME_DRIVER = (
    r"D:\Desktop\必然\CS转生\chromedriver-win64-134\chromedriver-win64\chromedriver.exe"
)

# 上海 + 在校生（实习向，与常见 Boss 参数一致）
CITY_SHANGHAI = "101020100"
EXP_STUDENT = "108"

# (搜索关键词, 写入 DB 的 category 分组标签)
SEARCH_TASKS: List[Tuple[str, str]] = [
    ("量化实习", "量化/资管"),
    ("量化研究员实习", "量化/资管"),
    ("资产管理实习", "量化/资管"),
    ("投资实习", "量化/资管"),
    ("金融工程实习", "证券/金工"),
    ("金工实习", "证券/金工"),
    ("证券实习", "证券/金工"),
    ("QD实习", "量化/资管"),
    ("QR实习", "量化/资管"),
    ("数据开发实习", "互联网"),
    ("数据分析实习", "互联网"),
    ("产品经理实习", "互联网"),
]

# 列表页职位卡片（SPA 搜索页常见 class，优先 job-card-left）
JOB_LIST_XPATHS = [
    '//div[contains(@class,"job-card-left")]',
    '//li[contains(@class,"job-card-wrapper")]',
    '//li[contains(@class,"job-card")]',
    '//div[contains(@class,"job-list-box")]//ul/li',
    '//*[@id="wrap"]//ul[contains(@class,"job-list-box")]/li',
    '//*[@id="wrap"]/div[2]/div[2]/div/div[1]/div[2]/ul/li',
    '//ul[contains(@class,"job-list-box")]/li',
]

JOB_LIST_CSS = (
    "div.job-card-left, li.job-card-wrapper, "
    "ul.job-list-box > li, div.search-job-result li.job-card-wrapper"
)


def _chrome_driver_executable() -> str:
    override = (os.environ.get("BOSS_CHROME_DRIVER_PATH") or "").strip()
    return override or _REF_CHROME_DRIVER


def _qualified_job_table() -> str:
    schema = (os.environ.get("PGJOB_SCHEMA") or "finance").strip() or "finance"
    if not schema.replace("_", "").isalnum():
        raise ValueError("PGJOB_SCHEMA 只能包含字母、数字与下划线")
    return f"{schema}.job_info"


def build_insert_sql() -> str:
    t = _qualified_job_table()
    return f"""
INSERT INTO {t}(
    category, sub_category, job_title, province, job_location, job_company,
    job_industry, job_finance, job_scale, job_welfare, job_salary_range,
    job_experience, job_education, job_skills, create_time
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


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
    "北京": ["北京"],
    "天津": ["天津"],
    "山西": ["太原", "阳泉", "晋城", "长治", "临汾", "运城", "忻州", "吕梁", "晋中", "大同", "朔州"],
    "河北": ["沧州", "石家庄", "唐山", "保定", "廊坊", "衡水", "邯郸", "邢台", "张家口", "辛集", "秦皇岛", "定州",
             "承德", "涿州"],
    "山东": ["济南", "淄博", "聊城", "德州", "滨州", "济宁", "菏泽", "枣庄", "烟台", "威海", "泰安", "青岛", "临沂",
             "莱芜", "东营", "潍坊", "日照"],
    "河南": ["郑州", "新乡", "鹤壁", "安阳", "焦作", "濮阳", "开封", "驻马店", "商丘", "三门峡", "南阳", "洛阳", "周口",
             "许昌", "信阳", "漯河", "平顶山", "济源"],
    "广东": ["珠海", "中山", "肇庆", "深圳", "清远", "揭阳", "江门", "惠州", "河源", "广州", "佛山", "东莞", "潮州",
             "汕尾", "梅州", "阳江", "云浮", "韶关", "湛江", "汕头", "茂名"],
    "浙江": ["舟山", "温州", "台州", "绍兴", "衢州", "宁波", "丽水", "金华", "嘉兴", "湖州", "杭州"],
    "宁夏": ["中卫", "银川", "吴忠", "石嘴山", "固原"],
    "江苏": ["镇江", "扬州", "盐城", "徐州", "宿迁", "无锡", "苏州", "南通", "南京", "连云港", "淮安", "常州", "泰州"],
    "湖南": ["长沙", "邵阳", "怀化", "株洲", "张家界", "永州", "益阳", "湘西", "娄底", "衡阳", "郴州", "岳阳", "常德",
             "湘潭"],
    "吉林": ["长春", "通化", "松原", "四平", "辽源", "吉林", "延边", "白山", "白城"],
    "福建": ["漳州", "厦门", "福州", "三明", "莆田", "宁德", "南平", "龙岩", "泉州"],
    "甘肃": ["张掖", "陇南", "兰州", "嘉峪关", "白银", "武威", "天水", "庆阳", "平凉", "临夏", "酒泉", "金昌", "甘南",
             "定西"],
    "陕西": ["榆林", "西安", "延安", "咸阳", "渭南", "铜川", "商洛", "汉中", "宝鸡", "安康"],
    "辽宁": ["营口", "铁岭", "沈阳", "盘锦", "辽阳", "锦州", "葫芦岛", "阜新", "抚顺", "丹东", "大连", "朝阳", "本溪",
             "鞍山"],
    "江西": ["鹰潭", "宜春", "上饶", "萍乡", "南昌", "景德镇", "吉安", "抚州", "新余", "九江", "赣州"],
    "黑龙江": ["伊春", "七台河", "牡丹江", "鸡西", "黑河", "鹤岗", "哈尔滨", "大兴安岭", "绥化", "双鸭山", "齐齐哈尔",
               "佳木斯", "大庆"],
    "安徽": ["宣城", "铜陵", "六安", "黄山", "淮南", "合肥", "阜阳", "亳州", "安庆", "池州", "宿州", "芜湖", "马鞍山",
             "淮北", "滁州", "蚌埠"],
    "湖北": ["孝感", "武汉", "十堰", "荆门", "黄冈", "襄阳", "咸宁", "随州", "黄石", "恩施", "鄂州", "荆州", "宜昌",
             "潜江", "天门", "神农架", "仙桃"],
    "青海": ["西宁", "海西", "海东", "玉树", "黄南", "海南", "海北", "果洛"],
    "新疆": ["乌鲁木齐", "克州", "阿勒泰", "五家渠", "石河子", "伊犁", "吐鲁番", "塔城", "克拉玛依", "喀什", "和田",
             "哈密", "昌吉", "博尔塔拉", "阿克苏", "巴音郭楞", "阿拉尔", "图木舒克", "铁门关"],
    "贵州": ["铜仁", "黔东南", "贵阳", "安顺", "遵义", "黔西南", "黔南", "六盘水", "毕节"],
    "四川": ["遂宁", "攀枝花", "眉山", "凉山", "成都", "巴中", "广安", "自贡", "甘孜", "资阳", "宜宾", "雅安", "内江",
             "南充", "绵阳", "泸州", "乐山", "广元", "德阳", "达州", "阿坝"],
    "上海": ["上海"],
    "广西": ["南宁", "贵港", "玉林", "梧州", "钦州", "柳州", "来宾", "贺州", "河池", "桂林", "防城港", "崇左", "北海",
             "百色"],
    "西藏": ["拉萨", "山南", "日喀则", "那曲", "林芝", "昌都", "阿里"],
    "云南": ["昆明", "红河", "大理", "玉溪", "昭通", "西双版纳", "文山", "曲靖", "普洱", "怒江", "临沧", "丽江",
             "迪庆", "德宏", "楚雄", "保山"],
    "内蒙古": ["呼和浩特", "乌兰察布", "兴安", "赤峰", "呼伦贝尔", "锡林郭勒", "乌海", "通辽", "巴彦淖尔", "阿拉善",
               "鄂尔多斯", "包头"],
    "海南": ["海口", "三沙", "三亚", "临高", "五指山", "陵水", "文昌", "万宁", "白沙", "乐东", "澄迈", "屯昌", "定安",
             "东方", "保亭", "琼中", "琼海", "儋州", "昌江"],
    "重庆": ["重庆"],
}


def build_search_url(query: str, page: int = 1) -> str:
    q = quote_plus(query)
    return (
        f"https://www.zhipin.com/web/geek/job?query={q}"
        f"&city={CITY_SHANGHAI}&experience={EXP_STUDENT}&page={page}"
    )


def init_webdriver() -> WebDriver:
    options = uc.ChromeOptions()
    if USE_HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=zh-CN")
    driver = uc.Chrome(
        options=options,
        driver_executable_path=_chrome_driver_executable(),
    )
    driver.__del__ = lambda: None  # type: ignore[method-assign]
    return driver


def _wait(driver: WebDriver, timeout: Optional[int] = None) -> WebDriverWait:
    return WebDriverWait(driver, timeout or WAIT_TIMEOUT)


def _page_looks_empty(driver: WebDriver) -> bool:
    src = driver.page_source
    markers = ("暂无相关职位", "没有更多", "无相关职位", "暂无数据", "职位已过期")
    return any(m in src for m in markers)


def is_security_challenge_page(driver: WebDriver) -> bool:
    """BOSS 对异常 IP / 自动化常见拦截：安全验证（极验）。"""
    try:
        title = driver.title or ""
    except Exception:
        title = ""
    if "安全验证" in title:
        return True
    try:
        src = driver.page_source or ""
    except Exception:
        return False
    return "page-verify" in src and "geetest" in src


def resolve_security_challenge(driver: WebDriver, retry_url: str) -> bool:
    """
    无头模式无法过验证，直接失败；
    有界面模式下暂停，用户在浏览器中完成验证后按回车继续。
    """
    if not is_security_challenge_page(driver):
        return True
    log.error(
        "当前页面为 BOSS「安全验证」（非职位列表）。"
        "无头模式无法自动通过；请使用: python boss_selenium.py --visible ..."
    )
    if USE_HEADLESS:
        return False
    print(
        "\n>>> 请在已打开的浏览器窗口中完成安全验证，"
        "看到职位列表后再回到此终端按【回车】继续 <<<\n"
    )
    try:
        input()
    except EOFError:
        return False
    try:
        driver.get(retry_url)
    except Exception as e:
        log.debug("重试加载: %s", e)
        return False
    polite_sleep(*SLEEP_AFTER_NAV)
    wait_for_search_shell(driver)
    if is_security_challenge_page(driver):
        log.error("验证后仍为安全验证页，请检查网络/IP 或稍后重试")
        return False
    return True


def wait_for_search_shell(driver: WebDriver) -> None:
    """SPA 搜索页：等待职位卡片渲染或明确空结果。"""
    wait = _wait(driver, min(WAIT_TIMEOUT, 45))
    try:
        wait.until(
            lambda d: bool(d.find_elements(By.CSS_SELECTOR, JOB_LIST_CSS))
            or _page_looks_empty(d)
            or ("加载中" not in d.page_source and len(d.page_source) > 80000)
        )
    except TimeoutException:
        log.warning("等待 SPA 职位列表超时，仍尝试解析")
    polite_sleep(*SLEEP_AFTER_SHELL)


def scroll_job_list_page(driver: WebDriver) -> None:
    for _ in range(2):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        polite_sleep(*SLEEP_SCROLL)


def find_job_li_elements(driver: WebDriver) -> List[WebElement]:
    els = driver.find_elements(By.CSS_SELECTOR, JOB_LIST_CSS)
    if els:
        log.info("使用 CSS 命中 %d 条: %s", len(els), JOB_LIST_CSS[:60])
        return els
    for xp in JOB_LIST_XPATHS:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            log.info("使用职位列表 XPath 命中 %d 条: %s", len(els), xp[:72])
            return els
    log.error("未找到职位列表节点，请检查选择器或页面是否改版/被拦截")
    return []


def province_for_location(job_location: str) -> str:
    city = job_location.split("·")[0].strip() if job_location else ""
    for p, cities in city_map.items():
        if city in cities:
            return p
    return ""


def _first_text(job: WebElement, xpaths: List[str], default: str = "") -> str:
    for xp in xpaths:
        try:
            t = job.find_element(By.XPATH, xp).text.strip()
            if t:
                return t
        except NoSuchElementException:
            continue
    return default


def _parse_job_card_left_div(
    job: WebElement, category_label: str, keyword: str, today: str
) -> Optional[tuple[Any, ...]]:
    """搜索页常见 div.job-card-left 结构。"""
    job_title = _first_text(
        job,
        [
            './/span[contains(@class,"job-name")]',
            './/a[contains(@class,"job-name")]//span',
            './/div[contains(@class,"job-title")]//span',
            './/span[@class="job-name"]',
        ],
    )
    if not job_title:
        return None

    job_location = _first_text(
        job,
        [
            './/span[contains(@class,"job-area")]',
            './/span[contains(@class,"job-area-wrap")]',
            './/span[contains(@class,"area")]',
        ],
    )
    job_company = _first_text(
        job,
        [
            './/h3[contains(@class,"company-name")]//a',
            './/span[contains(@class,"company-name")]',
            './/div[contains(@class,"company-info")]//a',
        ],
    )
    job_industry = _first_text(
        job, ['(.//ul[contains(@class,"company-tag-list")]/li)[1]'], "无"
    )
    job_finance = _first_text(
        job, ['(.//ul[contains(@class,"company-tag-list")]/li)[2]'], "无"
    )
    job_scale = _first_text(
        job, ['(.//ul[contains(@class,"company-tag-list")]/li)[3]'], "无"
    )
    job_welfare = _first_text(
        job,
        [
            './/div[contains(@class,"job-card-footer")]//span',
            './/div[contains(@class,"welfare")]',
        ],
        "无",
    )
    job_salary_range = _first_text(
        job,
        [
            './/span[contains(@class,"salary")]',
            './/span[contains(@class,"job-salary")]',
        ],
    )
    job_experience = _first_text(
        job, ['(.//ul[contains(@class,"tag-list")]/li)[1]'], "无"
    )
    job_education = _first_text(
        job, ['(.//ul[contains(@class,"tag-list")]/li)[2]'], "无"
    )
    skills = job.find_elements(
        By.XPATH, './/ul[contains(@class,"tag-list")]//li[position()>2]'
    )
    job_skills = ",".join(s.text.strip() for s in skills if s.text.strip()) or "无"

    province = province_for_location(job_location)
    return (
        category_label,
        keyword,
        job_title,
        province,
        job_location,
        job_company,
        job_industry,
        job_finance,
        job_scale,
        job_welfare,
        job_salary_range,
        job_experience,
        job_education,
        job_skills,
        today,
    )


def _parse_job_card_style(
    job: WebElement, category_label: str, keyword: str, today: str
) -> Optional[tuple[Any, ...]]:
    """新版 job-card-wrapper等卡片结构。"""
    try:
        title_el = job.find_element(
            By.XPATH,
            './/span[contains(@class,"job-name")]|.//a[contains(@class,"job-name")]',
        )
        job_title = title_el.text.strip()
    except NoSuchElementException:
        return None
    if not job_title:
        return None

    def _txt(xp: str, default: str = "无") -> str:
        try:
            return job.find_element(By.XPATH, xp).text.strip()
        except NoSuchElementException:
            return default

    job_location = _txt(
        './/span[contains(@class,"job-area")]|.//span[contains(@class,"job-area-wrap")]',
        "",
    )
    job_company = _txt('.//h3[contains(@class,"company-name")]//a|.//span[contains(@class,"company-name")]', "")
    job_industry = _txt('(.//ul[contains(@class,"company-tag-list")]/li)[1]', "无")
    job_finance = _txt('(.//ul[contains(@class,"company-tag-list")]/li)[2]', "无")
    job_scale = _txt('(.//ul[contains(@class,"company-tag-list")]/li)[3]', "无")
    job_welfare = _txt('.//div[contains(@class,"job-card-footer")]//div[contains(@class,"welfare")]', "无")
    job_salary_range = _txt(
        './/span[contains(@class,"salary")]|.//span[contains(@class,"job-salary")]',
        "",
    )
    job_experience = _txt('(.//ul[contains(@class,"tag-list")]/li)[1]', "无")
    job_education = _txt('(.//ul[contains(@class,"tag-list")]/li)[2]', "无")
    skills = job.find_elements(
        By.XPATH, './/ul[contains(@class,"tag-list")]//li[position()>2]'
    )
    job_skills = ",".join(s.text.strip() for s in skills if s.text.strip()) or "无"

    province = province_for_location(job_location)
    return (
        category_label,
        keyword,
        job_title,
        province,
        job_location,
        job_company,
        job_industry,
        job_finance,
        job_scale,
        job_welfare,
        job_salary_range,
        job_experience,
        job_education,
        job_skills,
        today,
    )


def parse_one_job(
    job: WebElement,
    category_label: str,
    keyword: str,
    today: str,
) -> Optional[tuple[Any, ...]]:
    """先尝试旧版 li 结构，再尝试 job-card / job-card-left 结构。"""
    try:
        cls = job.get_attribute("class") or ""
    except Exception:
        cls = ""
    if "job-card-left" in cls:
        return _parse_job_card_left_div(job, category_label, keyword, today)

    try:
        job_title = job.find_element(
            By.XPATH, "./div[1]/a/div[1]/span[1]"
        ).text.strip()
    except NoSuchElementException:
        alt = _parse_job_card_left_div(job, category_label, keyword, today)
        if alt:
            return alt
        return _parse_job_card_style(job, category_label, keyword, today)

    if not job_title:
        alt = _parse_job_card_left_div(job, category_label, keyword, today)
        if alt:
            return alt
        return _parse_job_card_style(job, category_label, keyword, today)

    try:
        job_location = job.find_element(
            By.XPATH, "./div[1]/a/div[1]/span[2]/span"
        ).text.strip()
        job_company = job.find_element(
            By.XPATH, "./div[1]/div/div[2]/h3/a"
        ).text.strip()
        job_industry = job.find_element(
            By.XPATH, "./div[1]/div/div[2]/ul/li[1]"
        ).text.strip()
        job_finance = job.find_element(
            By.XPATH, "./div[1]/div/div[2]/ul/li[2]"
        ).text.strip()
        try:
            job_scale = job.find_element(
                By.XPATH, "./div[1]/div/div[2]/ul/li[3]"
            ).text.strip()
        except NoSuchElementException:
            job_scale = "无"
        try:
            job_welfare = job.find_element(By.XPATH, "./div[2]/div").text.strip()
        except NoSuchElementException:
            job_welfare = "无"
        job_salary_range = job.find_element(
            By.XPATH, "./div[1]/a/div[2]/span[1]"
        ).text.strip()
        job_experience = job.find_element(
            By.XPATH, "./div[1]/a/div[2]/ul/li[1]"
        ).text.strip()
        job_education = job.find_element(
            By.XPATH, "./div[1]/a/div[2]/ul/li[2]"
        ).text.strip()
        try:
            job_skills = ",".join(
                s.text.strip()
                for s in job.find_elements(By.XPATH, "./div[2]/ul/li")
            )
        except NoSuchElementException:
            job_skills = "无"
    except NoSuchElementException:
        alt = _parse_job_card_left_div(job, category_label, keyword, today)
        if alt:
            return alt
        return _parse_job_card_style(job, category_label, keyword, today)

    province = province_for_location(job_location)
    return (
        category_label,
        keyword,
        job_title,
        province,
        job_location,
        job_company,
        job_industry,
        job_finance,
        job_scale,
        job_welfare,
        job_salary_range,
        job_experience,
        job_education,
        job_skills or "无",
        today,
    )


def restart_driver(old: Optional[WebDriver]) -> WebDriver:
    if old is not None:
        try:
            old.quit()
        except Exception as e:
            log.debug("quit 旧 driver: %s", e)
        polite_sleep(*SLEEP_AFTER_RESTART)
    return init_webdriver()


def get_pg_config() -> dict:
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD") or "pg621",
        "db": os.environ.get("PGDATABASE", "postgres"),
    }


def resolve_search_tasks(keywords_arg: Optional[str]) -> List[Tuple[str, str]]:
    if keywords_arg:
        parts = [p.strip() for p in keywords_arg.split(",") if p.strip()]
        return [(p, "自定义") for p in parts]
    return list(SEARCH_TASKS)


def run_scrape(
    dry_run: bool, tasks: Sequence[Tuple[str, str]], max_pages: int
) -> None:
    today = datetime.date.today().strftime("%Y-%m-%d")
    db: Optional[DBUtils] = None
    insert_sql = build_insert_sql()
    if not dry_run:
        cfg = get_pg_config()
        db = DBUtils(
            cfg["host"],
            cfg["user"],
            cfg["password"],
            cfg["db"],
            port=cfg["port"],
        )
        log.info(
            "已连接 PostgreSQL: %s/%s 表 %s",
            cfg["host"],
            cfg["db"],
            _qualified_job_table(),
        )

    seen: set[tuple[str, str, str]] = set()
    driver: Optional[WebDriver] = None
    task_list = list(tasks)
    try:
        driver = init_webdriver()
        for kw_idx, (keyword, category_label) in enumerate(task_list):
            if kw_idx > 0 and kw_idx % RESTART_EVERY == 0:
                log.info("已处理 %d 个关键词，重启浏览器…", kw_idx)
                driver = restart_driver(driver)

            log.info("%s 关键词 [%s] 分组=%s", today, keyword, category_label)
            page = 1
            prev_first_title: Optional[str] = None

            while page <= max_pages:
                url = build_search_url(keyword, page)
                log.info("GET %s", url)
                driver.get(url)
                polite_sleep(*SLEEP_AFTER_NAV)
                wait_for_search_shell(driver)
                if not resolve_security_challenge(driver, url):
                    log.error("因安全验证未通过，中止抓取（可改 --visible 后手动过检重试）")
                    return
                scroll_job_list_page(driver)

                job_detail = find_job_li_elements(driver)
                if not job_detail:
                    try:
                        with open("boss_last_page.html", "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        log.warning(
                            "关键词 %s 第 %d 页无列表，已保存 boss_last_page.html",
                            keyword,
                            page,
                        )
                    except OSError as e:
                        log.debug("保存调试 HTML 失败: %s", e)
                    log.info("关键词 %s 第 %d 页无列表，结束分页", keyword, page)
                    break

                first_probe = parse_one_job(
                    job_detail[0], category_label, keyword, today
                )
                first_title = first_probe[2] if first_probe else None
                if (
                    first_title
                    and prev_first_title
                    and first_title == prev_first_title
                ):
                    log.info("关键词 %s 第 %d 页与上页首条重复，停止分页", keyword, page)
                    break
                prev_first_title = first_title

                parsed_page = 0
                for j_i, job in enumerate(job_detail):
                    if (
                        SLEEP_EVERY_N_JOBS > 0
                        and j_i > 0
                        and j_i % SLEEP_EVERY_N_JOBS == 0
                    ):
                        log.debug("已解析 %d 条卡片，批量停顿", j_i)
                        polite_sleep(*SLEEP_BATCH_PAUSE)
                    polite_sleep(*SLEEP_PER_JOB_ROW)
                    row = parse_one_job(job, category_label, keyword, today)
                    if row is None:
                        continue
                    dedupe_key = (row[2], row[5], row[4])
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    parsed_page += 1
                    if dry_run:
                        log.info(
                            "DRY [行] %s | %s | %s | %s",
                            row[2],
                            row[4],
                            row[5],
                            row[10],
                        )
                    else:
                        assert db is not None
                        db.insert_data(insert_sql, row)
                    print(
                        category_label,
                        keyword,
                        *row[2:14],
                        sep=" | ",
                    )
                log.info(
                    "关键词 %s 第 %d 页 解析 %d 条（累计去重后总条数 %d）",
                    keyword,
                    page,
                    parsed_page,
                    len(seen),
                )

                if parsed_page == 0:
                    break

                page += 1
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
        if driver is not None:
            try:
                driver.quit()
            except Exception as e:
                log.debug("driver.quit: %s", e)
        if db is not None:
            db.close()
            log.info("数据库连接已关闭")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Boss直聘 上海在校生/实习 针对性关键词搜索抓取"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="不写库，仅打印/日志",
    )
    parser.add_argument(
        "--keywords",
        type=str,
        default="",
        help="逗号分隔关键词，覆盖默认 SEARCH_TASKS；例：量化实习,数据分析实习",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="显示浏览器窗口",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="每个关键词最多翻页数，0 表示使用 BOSS_SCRAPER_MAX_PAGES（默认 10）",
    )
    args = parser.parse_args()
    global USE_HEADLESS
    if args.visible:
        USE_HEADLESS = False

    tasks = resolve_search_tasks(args.keywords.strip() or None)
    max_pages = args.max_pages if args.max_pages > 0 else MAX_PAGES_PER_KEYWORD
    log.info(
        "headless=%s wait=%ds restart_every=%d max_pages=%d dry_run=%s tasks=%d",
        USE_HEADLESS,
        WAIT_TIMEOUT,
        RESTART_EVERY,
        max_pages,
        args.dry_run,
        len(tasks),
    )
    log.info(
        "间隔(秒,已×mult=%s): after_nav=%s after_shell=%s scroll=%s "
        "between_pages=%s between_keywords=%s after_restart=%s "
        "per_row=%s every_n=%s batch_pause=%s",
        SLEEP_MULT,
        SLEEP_AFTER_NAV,
        SLEEP_AFTER_SHELL,
        SLEEP_SCROLL,
        SLEEP_BETWEEN_PAGES,
        SLEEP_BETWEEN_KEYWORDS,
        SLEEP_AFTER_RESTART,
        SLEEP_PER_JOB_ROW,
        SLEEP_EVERY_N_JOBS,
        SLEEP_BATCH_PAUSE,
    )
    run_scrape(dry_run=args.dry_run, tasks=tasks, max_pages=max_pages)


if __name__ == "__main__":
    main()
