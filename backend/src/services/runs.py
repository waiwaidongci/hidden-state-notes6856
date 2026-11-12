from datetime import datetime,timezone
from sqlalchemy import select
from fastapi import HTTPException
from src.models import ModelProfile,ObservationTrace,InferenceRun
from src.utils.serialize import row
from src.worker import analyze_run
from .audit import record

def _now():
 return datetime.now(timezone.utc).isoformat()
def _enqueue(run):
 analyze_run.delay(run.id,run.attempt_no)
def _append_history(run,entry):
 history=list(run.attempt_history or [])
 history.append(entry)
 run.attempt_history=history
def create_run(db,data,actor):
 profile=db.get(ModelProfile,data.profile_id)
 if not profile or profile.study_id!=data.study_id:raise HTTPException(422,'模型版本不属于该研究')
 ids=data.trace_ids
 if len(ids)!=len(set(ids)):raise HTTPException(422,'轨迹ID不能重复')
 traces=list(db.scalars(select(ObservationTrace).where(ObservationTrace.id.in_(ids),ObservationTrace.study_id==data.study_id).order_by(ObservationTrace.reference)))
 if len(traces)!=len(ids):raise HTTPException(422,'轨迹不属于该研究或不存在')
 if db.scalar(select(InferenceRun).where(InferenceRun.name==data.name)):raise HTTPException(409,'运行名已存在')
 run=InferenceRun(study_id=data.study_id,profile_id=profile.id,name=data.name,profile_snapshot=row(profile),trace_snapshot=[row(t) for t in traces],created_by=actor,attempt_no=1,attempt_history=[{'attempt':1,'phase':'queued','status':'queued','at':_now(),'error':None}])
 db.add(run);db.commit()
 try:
  _enqueue(run)
 except Exception:
  run.status='failed';run.error='队列不可用，请稍后重试'
  _append_history(run,{'attempt':1,'phase':'failed','status':'failed','at':_now(),'error':run.error})
  db.commit();raise HTTPException(503,run.error)
 record(db,actor,'run.queued',{'run_id':run.id,'traces':len(traces),'attempt':1});return row(run)
def retry_run(db,run_id,actor):
 # 行锁串行化同一运行的并发重试：多人同时点重试只产生一轮新执行
 run=db.scalar(select(InferenceRun).where(InferenceRun.id==run_id).with_for_update())
 if not run:raise HTTPException(404,'运行不存在')
 if run.status=='completed':raise HTTPException(409,'运行已成功，无需重试')
 if run.status in ('queued','running'):raise HTTPException(409,'该轮推断仍在进行，请勿重复重试')
 # 仅 failed 可原地重试；沿用首次排队冻结的模型与观测快照，之后修订观测也不改变输入
 next_attempt=run.attempt_no+1
 run.attempt_no=next_attempt
 run.status='queued';run.error=None;run.result=None;run.total_log_likelihood=None
 _append_history(run,{'attempt':next_attempt,'phase':'retry','status':'queued','at':_now(),'error':None,'retried_by':actor})
 db.commit()
 try:
  _enqueue(run)
 except Exception:
  run=db.get(InferenceRun,run_id)
  run.status='failed';run.error='队列不可用，请稍后重试'
  _append_history(run,{'attempt':next_attempt,'phase':'failed','status':'failed','at':_now(),'error':run.error})
  db.commit();raise HTTPException(503,run.error)
 record(db,actor,'run.retried',{'run_id':run_id,'attempt':next_attempt,'previous_attempts':next_attempt-1})
 return row(run)
