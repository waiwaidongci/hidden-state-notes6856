"""worker 围栏语义：重复投递只跑一轮，旧执行迟到的成功/失败不能覆盖新一轮结果。"""
from src.models import InferenceRun,AuditLog
from src.worker import execute_attempt
from backend.tests.conftest import make_run
def _fresh(session,run_id):
 s=session['new_session']()
 try:return s.get(InferenceRun,run_id)
 finally:s.close()
def test_first_attempt_success_records_history(session):
 run=make_run(session)
 other=session['new_session']()
 execute_attempt(other,run.id,1)
 other.close()
 saved=_fresh(session,run.id)
 assert saved.status=='completed'
 assert saved.attempt_no==1
 assert saved.error is None
 assert saved.result is not None
 phases=[h['phase'] for h in saved.attempt_history]
 assert phases==['queued','completed']
 assert all(h['attempt']==1 for h in saved.attempt_history)
def test_duplicate_delivery_runs_only_once(session,monkeypatch):
 run=make_run(session)
 calls={'n':0}
 from src.services import analysis
 real=analysis.analyze
 def counting(profile,traces):
  calls['n']+=1
  return real(profile,traces)
 monkeypatch.setattr('src.worker.analyze',counting)
 s1=session['new_session']();s2=session['new_session']()
 execute_attempt(s1,run.id,1)
 # 队列重复投递：同一轮任务再次到达，直接幂等返回
 execute_attempt(s2,run.id,1)
 execute_attempt(s2,run.id,1)
 s1.close();s2.close()
 saved=_fresh(session,run.id)
 assert saved.status=='completed'
 assert calls['n']==1
def test_duplicate_delivery_failed_attempt_is_idempotent(session,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 calls={'n':0}
 def boom(profile,traces):
  calls['n']+=1
  raise ValueError('不可能序列：第1码在固定模型下概率为0')
 monkeypatch.setattr('src.worker.analyze',boom)
 s1=session['new_session']();s2=session['new_session']()
 execute_attempt(s1,run.id,1)
 execute_attempt(s2,run.id,1)
 s1.close();s2.close()
 saved=_fresh(session,run.id)
 assert saved.status=='failed'
 assert calls['n']==1
 failed=[h for h in saved.attempt_history if h['phase']=='failed']
 assert len(failed)==1 and failed[0]['error']
def test_stale_success_cannot_overwrite_new_attempt(session,monkeypatch):
 """旧轮次（attempt 1）在新一轮（attempt 2）排队后才成功：其成功结果必须被丢弃。"""
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 # 首轮失败
 s=session['new_session']()
 execute_attempt(s,run.id,1);s.close()
 saved=_fresh(session,run.id)
 assert saved.status=='failed'
 first_error=saved.error
 # 人工修复快照模拟“重试使用相同冻结输入、但本轮可成功”——实际重试不改快照，
 # 这里通过把 analyze 在第二轮替换为成功实现，验证的是围栏而非算法
 from src.services.runs import retry_run
 db=session['db']
 retry_run(db,run.id,'analyst@yanxi.demo')
 saved=_fresh(session,run.id)
 assert saved.attempt_no==2 and saved.status=='queued'
 # 旧执行迟到成功：用首轮 attempt_no 调用，且让其算出成功结果
 monkeypatch.setattr('src.worker.analyze',
                     lambda profile,traces:{'total_log_likelihood':-1.0,'trace_count':1})
 stale=session['new_session']()
 execute_attempt(stale,run.id,1)   # attempt_no=1 已过期
 stale.close()
 saved=_fresh(session,run.id)
 assert saved.attempt_no==2
 assert saved.status=='queued'      # 未被旧成功覆盖
 assert saved.result is None and saved.total_log_likelihood is None
 # 新一轮真正执行成功
 current=session['new_session']()
 execute_attempt(current,run.id,2)
 current.close()
 saved=_fresh(session,run.id)
 assert saved.status=='completed' and saved.attempt_no==2
 assert saved.total_log_likelihood==-1.0
 # 首轮错误仍保留在历史中
 assert any(h['attempt']==1 and h['phase']=='failed' and h['error']==first_error
            for h in saved.attempt_history)
 assert any(h['attempt']==2 and h['phase']=='retry' for h in saved.attempt_history)
 assert any(h['attempt']==2 and h['phase']=='completed' for h in saved.attempt_history)
def test_stale_failure_cannot_overwrite_new_success(session,monkeypatch):
 """旧轮次迟到的失败不能把新一轮已成功的运行打回 failed。"""
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 from src.services.runs import retry_run
 retry_run(session['db'],run.id,'analyst@yanxi.demo')
 # 新一轮成功
 monkeypatch.setattr('src.worker.analyze',
                     lambda profile,traces:{'total_log_likelihood':-2.5,'trace_count':1})
 cur=session['new_session']();execute_attempt(cur,run.id,2);cur.close()
 saved=_fresh(session,run.id)
 assert saved.status=='completed' and saved.attempt_no==2
 # 旧执行迟到并报错
 monkeypatch.setattr('src.worker.analyze',
                     lambda profile,traces:(_ for _ in ()).throw(RuntimeError('旧失败')))
 stale=session['new_session']();execute_attempt(stale,run.id,1);stale.close()
 saved=_fresh(session,run.id)
 assert saved.status=='completed'
 assert saved.attempt_no==2
 assert saved.total_log_likelihood==-2.5
 assert saved.error is None
def test_running_duplicate_delivery_does_not_restart(session,monkeypatch):
 """同一轮正在 running 时重复投递：不重启分析。"""
 run=make_run(session)
 calls={'n':0}
 from src.services import analysis
 real=analysis.analyze
 def counting(profile,traces):
  calls['n']+=1
  # 分析进行中模拟重复消息到达：另起会话看到 running 应直接返回
  dup=session['new_session']()
  execute_attempt(dup,run.id,1)
  dup.close()
  return real(profile,traces)
 monkeypatch.setattr('src.worker.analyze',counting)
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 assert calls['n']==1
 saved=_fresh(session,run.id)
 assert saved.status=='completed'
def test_missing_run_is_noop(session):
 s=session['new_session']()
 execute_attempt(s,99999,1)  # 不抛异常
 s.close()
def test_worker_audit_carry_attempt_number(session):
 run=make_run(session)
 s=session['new_session']();execute_attempt(s,run.id,1);s.close()
 logs=session['db'].query(AuditLog).filter_by(action='run.completed').all()
 assert logs and logs[0].detail['attempt']==1
