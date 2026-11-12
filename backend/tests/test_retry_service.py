"""重试服务：原地重试、快照冻结不变、并发/重复重试只产生一轮、状态机约束。"""
import pytest
from fastapi import HTTPException
from sqlalchemy import select
from src.models import InferenceRun,ObservationTrace,AuditLog
from src.services.runs import create_run,retry_run
from src.schemas.run import RunInput
from backend.tests.conftest import make_run
def _fresh(session,run_id):
 s=session['new_session']()
 try:return s.get(InferenceRun,run_id)
 finally:s.close()
def test_retry_keeps_original_snapshots_after_trace_revision(session):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 # 首轮先失败，页面才提供原地重试
 from src.worker import execute_attempt
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 original_profile_snap=dict(_fresh(session,run.id).profile_snapshot)
 original_trace_snap=[dict(t) for t in _fresh(session,run.id).trace_snapshot]
 # 源观测随后被修订（模拟其他分析员改正轨迹）
 trace=session['db'].get(ObservationTrace,session['bad_trace'].id)
 trace.tokens=['A','A','B']
 session['db'].commit()
 db=session['db']
 retry_run(db,run.id,'analyst@yanxi.demo')
 saved=_fresh(session,run.id)
 assert saved.attempt_no==2
 assert saved.status=='queued'
 # 重试输入仍是首次排队冻结的快照，不含修订后的观测
 assert saved.profile_snapshot==original_profile_snap
 assert saved.trace_snapshot==original_trace_snap
 assert saved.trace_snapshot[0]['tokens']==['C']
 # 结果字段已清空，等待新一轮
 assert saved.result is None and saved.error is None
 assert saved.total_log_likelihood is None
 # 历史含重试记录
 retry_entries=[h for h in saved.attempt_history if h['phase']=='retry']
 assert len(retry_entries)==1
 assert retry_entries[0]['attempt']==2
 assert retry_entries[0]['retried_by']=='analyst@yanxi.demo'
def test_each_failure_is_preserved_across_rounds(session,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 # 第一轮失败
 monkeypatch.setattr('src.worker.analyze',
                     lambda p,t:(_ for _ in ()).throw(ValueError('第一轮错误')))
 from src.worker import execute_attempt
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 saved=_fresh(session,run.id)
 assert saved.status=='failed' and '第一轮错误' in saved.error
 # 重试，第二轮也失败
 retry_run(session['db'],run.id,'analyst@yanxi.demo')
 monkeypatch.setattr('src.worker.analyze',
                     lambda p,t:(_ for _ in ()).throw(ValueError('第二轮错误')))
 s=session['new_session']();execute_attempt(s,run.id,2);s.close()
 saved=_fresh(session,run.id)
 assert saved.attempt_no==2 and saved.status=='failed'
 assert '第二轮错误' in saved.error
 failures=saved.attempt_history
 by_attempt={}
 for h in failures:
  by_attempt.setdefault(h['attempt'],[]).append(h)
 assert '第一轮错误' in [h['error'] for h in by_attempt[1]]
 assert '第二轮错误' in [h['error'] for h in by_attempt[2]]
def test_concurrent_retries_create_single_new_attempt(session):
 """多人并发重试：串行提交两次重试请求，第二次必须 409，只排队一轮。"""
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 from src.worker import execute_attempt
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 db=session['db']
 first=retry_run(db,run.id,'analyst@yanxi.demo')
 assert first['attempt_no']==2 and first['status']=='queued'
 with pytest.raises(HTTPException) as exc:
  retry_run(db,run.id,'admin@yanxi.demo')
 assert exc.value.status_code==409
 saved=_fresh(session,run.id)
 assert saved.attempt_no==2
 retries=[h for h in saved.attempt_history if h['phase']=='retry']
 assert len(retries)==1
def test_retry_while_running_is_rejected(session):
 run=make_run(session)
 attached=session['db'].get(InferenceRun,run.id)
 attached.status='running';session['db'].commit()
 with pytest.raises(HTTPException) as exc:
  retry_run(session['db'],run.id,'analyst@yanxi.demo')
 assert exc.value.status_code==409
 saved=_fresh(session,run.id)
 assert saved.attempt_no==1
def test_retry_completed_run_is_rejected_and_result_untouched(session):
 run=make_run(session)
 from src.worker import execute_attempt
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 saved=_fresh(session,run.id)
 assert saved.status=='completed'
 frozen_result=saved.result
 with pytest.raises(HTTPException) as exc:
  retry_run(session['db'],run.id,'analyst@yanxi.demo')
 assert exc.value.status_code==409
 saved=_fresh(session,run.id)
 assert saved.status=='completed'
 assert saved.attempt_no==1
 assert saved.result==frozen_result
def test_retry_unknown_run_404(session):
 with pytest.raises(HTTPException) as exc:
  retry_run(session['db'],99999,'analyst@yanxi.demo')
 assert exc.value.status_code==404
def test_retry_enqueues_with_new_attempt_number(session,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 from src.worker import execute_attempt
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 captured=[]
 class FakeTask:
  def delay(self,run_id,attempt_no):captured.append((run_id,attempt_no))
 monkeypatch.setattr('src.services.runs.analyze_run',FakeTask())
 retry_run(session['db'],run.id,'analyst@yanxi.demo')
 assert captured==[(run.id,2)]
 logs=session['db'].scalars(select(AuditLog).where(AuditLog.action=='run.retried')).all()
 assert len(logs)==1 and logs[0].detail['attempt']==2
def test_retry_when_queue_unavailable_marks_failed_with_history(session,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 from src.worker import execute_attempt
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 class BrokenTask:
  def delay(self,*a,**k):raise RuntimeError('broker down')
 monkeypatch.setattr('src.services.runs.analyze_run',BrokenTask())
 with pytest.raises(HTTPException) as exc:
  retry_run(session['db'],run.id,'analyst@yanxi.demo')
 assert exc.value.status_code==503
 saved=_fresh(session,run.id)
 assert saved.status=='failed' and saved.attempt_no==2
 assert any(h['phase']=='retry' and h['attempt']==2 for h in saved.attempt_history)
 assert any(h['phase']=='failed' and h['attempt']==2 for h in saved.attempt_history)
def test_create_run_records_initial_attempt_history(session,monkeypatch):
 captured=[]
 class FakeTask:
  def delay(self,run_id,attempt_no):captured.append((run_id,attempt_no))
 monkeypatch.setattr('src.services.runs.analyze_run',FakeTask())
 data=RunInput(study_id=session['study'].id,profile_id=session['profile'].id,
               name='新建推断-001',trace_ids=[session['ok_trace'].id])
 out=create_run(session['db'],data,'analyst@yanxi.demo')
 assert out['attempt_no']==1
 assert out['attempt_history'][0]['phase']=='queued'
 assert captured==[(out['id'],1)]
