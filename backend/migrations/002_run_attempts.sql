-- 原地重试（retry in place）需要在既有 inference_runs 上补两列。
-- 全新卷由 SQLAlchemy create_all 直接建出含新列的表，无需执行本文件；
-- 本文件仅为已存在数据卷提供手工升级口径，不构成生产迁移链。
-- MySQL 8.0.29+ / MariaDB 支持 ADD COLUMN IF NOT EXISTS；更早版本请去掉 IF NOT EXISTS 并自行确认列不存在。
ALTER TABLE inference_runs
 ADD COLUMN IF NOT EXISTS attempt_no INTEGER NOT NULL DEFAULT 1,
 ADD COLUMN IF NOT EXISTS attempt_history JSON NOT NULL DEFAULT ('[]');
-- 既有成功/失败运行视为第 1 轮；失败行可随后通过 POST /runs/{id}/retry 原地重试。
