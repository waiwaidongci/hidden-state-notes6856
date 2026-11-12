import json
from pathlib import Path
from celery import Celery
from sqlalchemy import select,update
from src.config.database import SessionLocal
from src.config.settings import REDIS_URL,ARTIFACT_DIR
from src.models import InferenceRun
from src.services.analysis import analyze
from src.services.audit import record
app=Celery('hidden_state_notes',broker=REDIS_URL,backend=REDIS_URL)
app.conf.update(task_track_started=True,worker_prefetch_multiplier=1,task_acks_late=True)
def _now():
 from datetime import datetime,timezone
 return datetime.now(timezone.utc).isoformat()
def _append_history(run,entry):
 history=list(run.attempt_history or [])
 history.append(entry)
 run.attempt_history=history
def _attempt_terminated(run,attempt_no):
 """该轮是否已写入终态记录（completed/failed）：终态后的重复投递不再执行。"""
 return any(h.get('attempt')==attempt_no and h.get('phase') in ('completed','failed')
            for h in (run.attempt_history or []))
def _stale(run,attempt_no):
 return run is None or run.attempt_no!=attempt_no or run.status=='completed' \
        or _attempt_terminated(run,attempt_no)
def execute_attempt(db,run_id,attempt_no):
 """执行某一轮推断。以 attempt_no 为围栏：重复投递与旧轮次迟到结果都不会覆盖新一轮。"""
 run=db.get(InferenceRun,run_id)
 if _stale(run,attempt_no):return
 # 原子认领：queued -> running。多 worker 重复投递同一轮时只有一个 UPDATE 生效，只执行一轮
 claimed=db.execute(update(InferenceRun).where(
  InferenceRun.id==run_id,InferenceRun.attempt_no==attempt_no,
  InferenceRun.status=='queued').values(status='running')).rowcount
 if not any(h.get('attempt')==attempt_no for h in (run.attempt_history or [])):
  _append_history(run,{'attempt':attempt_no,'phase':'queued','status':'queued',
                       'at':_now(),'error':None})
 db.commit()
 if claimed==0:return
 db.expire_all()
 try:
  # 始终使用首次排队冻结的 profile_snapshot/trace_snapshot，不读取可能已被修订的源记录
  run=db.get(InferenceRun,run_id)
  result=analyze(run.profile_snapshot,run.trace_snapshot)
  Path(ARTIFACT_DIR).mkdir(parents=True,exist_ok=True)
  Path(ARTIFACT_DIR,f'hmm-{run_id}.json').write_text(json.dumps(result,ensure_ascii=False,allow_nan=False))
  # 提交前行锁围栏：长任务期间若新一轮已排队，旧结果丢弃
  run=db.scalar(select(InferenceRun).where(InferenceRun.id==run_id).with_for_update())
  if _stale(run,attempt_no):db.rollback();return
  run.result=result
  run.total_log_likelihood=result['total_log_likelihood']
  run.status='completed';run.error=None
  _append_history(run,{'attempt':attempt_no,'phase':'completed','status':'completed',
                       'at':_now(),'error':None})
  db.commit()
  record(db,'worker','run.completed',{'run_id':run_id,'attempt':attempt_no,'trace_count':result['trace_count']})
 except Exception as error:
  db.rollback()
  run=db.scalar(select(InferenceRun).where(InferenceRun.id==run_id).with_for_update())
  if _stale(run,attempt_no):return
  message=str(error)[:1000]
  run.status='failed';run.error=message
  _append_history(run,{'attempt':attempt_no,'phase':'failed','status':'failed',
                       'at':_now(),'error':message})
  db.commit()
  record(db,'worker','run.failed',{'run_id':run_id,'attempt':attempt_no,'error':message})
@app.task(name='hmm.analyze',soft_time_limit=120,time_limit=150)
def analyze_run(id,attempt_no=1):
 with SessionLocal() as db:
  execute_attempt(db,id,attempt_no)
