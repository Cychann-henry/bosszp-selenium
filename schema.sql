--与金融中台主库一致：库名 postgres（或你自定义的 PGDATABASE），表在 schema finance 下
-- 在 psql 中：\c postgres  后执行本脚本

CREATE SCHEMA IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.job_info (
    id SERIAL PRIMARY KEY,
    category VARCHAR(255),
    sub_category VARCHAR(255),
    job_title VARCHAR(255),
    province VARCHAR(100),
    job_location VARCHAR(255),
    job_company VARCHAR(255),
    job_industry VARCHAR(255),
    job_finance VARCHAR(255),
    job_scale VARCHAR(255),
    job_welfare TEXT,
    job_salary_range VARCHAR(255),
    job_experience VARCHAR(255),
    job_education VARCHAR(255),
    job_skills TEXT,
    job_jd TEXT,
    create_time VARCHAR(50)
);

COMMENT ON TABLE finance.job_info IS 'Boss直聘岗位抓取';
COMMENT ON COLUMN finance.job_info.category IS '一级分类';
COMMENT ON COLUMN finance.job_info.sub_category IS '二级分类';
COMMENT ON COLUMN finance.job_info.job_title IS '岗位名称';
COMMENT ON COLUMN finance.job_info.province IS '省份';
COMMENT ON COLUMN finance.job_info.job_location IS '工作位置';
COMMENT ON COLUMN finance.job_info.job_company IS '企业名称';
COMMENT ON COLUMN finance.job_info.job_industry IS '行业类型';
COMMENT ON COLUMN finance.job_info.job_finance IS '融资情况';
COMMENT ON COLUMN finance.job_info.job_scale IS '企业规模';
COMMENT ON COLUMN finance.job_info.job_welfare IS '企业福利';
COMMENT ON COLUMN finance.job_info.job_salary_range IS '薪资范围';
COMMENT ON COLUMN finance.job_info.job_experience IS '工作年限';
COMMENT ON COLUMN finance.job_info.job_education IS '学历要求';
COMMENT ON COLUMN finance.job_info.job_skills IS '技能要求';
COMMENT ON COLUMN finance.job_info.job_jd IS '职位详情页 JD（可选抓取）';
COMMENT ON COLUMN finance.job_info.create_time IS '抓取时间';

-- 若表在加 job_jd 列之前已创建，在 psql 中执行一次即可补列（重复执行安全）
ALTER TABLE finance.job_info ADD COLUMN IF NOT EXISTS job_jd TEXT;
