"""HTTP 层：重试端点的鉴权、409 重复投递语义与端到端轮次流程。"""
from datetime import datetime,timedelta,timezone
import jwt
import pytest
from fastapi.testclient import TestClient
from src.config import database as db_config
from src.config.settings import JWT_SECRET
from src.main import app
from src.models import User
from backend.tests.conftest import make_run
def _token(uid):
 return jwt.encode({'sub':str(uid),
                    'exp':datetime.now(timezone.utc)+timedelta(hours=8)},
                   JWT_SECRET,algorithm='HS256')
def _fail_first_attempt(run_id):
 from src.worker import execute_attempt
 s=db_config.SessionLocal();execute_attempt(s,run_id,1);s.close()
@pytest.fixture
def client(session):
 # 复用 conftest 的内存库：TestClient 的依赖注入改用同一 SessionLocal
 def override_get_db():
  db=db_config.SessionLocal()
  try:yield db
  finally:db.close()
 from src.config.database import get_db
 app.dependency_overrides[get_db]=override_get_db
 # main.py 在导入时已绑定 MySQL engine；lifespan 的 create_all 改指向测试内存库
 import src.main as main_module
 main_module.engine=db_config.engine
 # 限流中间件依赖 Redis 基础设施，测试中直通；鉴权/路由/服务均为真实链路
 from src.middlewares.rate_limit import RateLimitMiddleware
 app.user_middleware=[m for m in app.user_middleware
                      if m.cls is not RateLimitMiddleware]
 app.middleware_stack=app.build_middleware_stack()
 with TestClient(app,raise_server_exceptions=True) as c:
  yield c
 app.dependency_overrides.clear()
 app.middleware_stack=None
def _auth(user):
 uid=getattr(user,'id',user)
 return {'Authorization':'Bearer '+_token(uid)}
def test_retry_endpoint_happy_path(session,client,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 _fail_first_attempt(run.id)
 captured=[]
 class FakeTask:
  def delay(self,run_id,attempt_no):captured.append((run_id,attempt_no))
 monkeypatch.setattr('src.services.runs.analyze_run',FakeTask())
 r=client.post(f'/runs/{run.id}/retry',headers=_auth(session['user']))
 assert r.status_code==200,r.text
 body=r.json()
 assert body['attempt_no']==2 and body['status']=='queued'
 assert captured==[(run.id,2)]
def test_duplicate_retry_requests_second_is_409(session,client,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 _fail_first_attempt(run.id)
 class FakeTask:
  def delay(self,*a,**k):pass
 monkeypatch.setattr('src.services.runs.analyze_run',FakeTask())
 h=_auth(session['user'])
 first=client.post(f'/runs/{run.id}/retry',headers=h)
 second=client.post(f'/runs/{run.id}/retry',headers=h)
 assert first.status_code==200
 assert second.status_code==409
 body=client.get(f'/runs/{run.id}',headers=h).json()
 assert body['attempt_no']==2
 assert len([x for x in body['attempt_history'] if x['phase']=='retry'])==1
def test_retry_requires_authentication(session,client):
 r=client.post(f'/runs/{1}/retry')
 assert r.status_code==401
def test_viewer_cannot_retry(session,client):
 viewer=User(email='viewer@yanxi.demo',name='程沅',role='viewer',password_hash='x')
 session['db'].add(viewer);session['db'].commit()
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 _fail_first_attempt(run.id)
 r=client.post(f'/runs/{run.id}/retry',headers=_auth(viewer))
 assert r.status_code==403
def test_retry_unknown_run_404(session,client):
 r=client.post('/runs/99999/retry',headers=_auth(session['user']))
 assert r.status_code==404
def test_completed_run_retry_409_and_snapshot_kept(session,client):
 run=make_run(session)
 from src.worker import execute_attempt
 s=db_config.SessionLocal();execute_attempt(s,run.id,1);s.close()
 before=client.get(f'/runs/{run.id}',headers=_auth(session['user'])).json()
 r=client.post(f'/runs/{run.id}/retry',headers=_auth(session['user']))
 assert r.status_code==409
 after=client.get(f'/runs/{run.id}',headers=_auth(session['user'])).json()
 assert after['status']=='completed'
 assert after['result']==before['result']
 assert after['attempt_no']==1
def test_end_to_end_fail_retry_succeed(session,client,monkeypatch):
 run=make_run(session,profile=session['bad_profile'],traces=[session['bad_trace']])
 # 首轮真实失败
 from src.worker import execute_attempt
 s=db_config.SessionLocal();execute_attempt(s,run.id,1);s.close()
 failed=client.get(f'/runs/{run.id}',headers=_auth(session['user'])).json()
 assert failed['status']=='failed' and failed['error']
 # 页面原地重试
 class FakeTask:
  def delay(self,*a,**k):pass
 monkeypatch.setattr('src.services.runs.analyze_run',FakeTask())
 r=client.post(f'/runs/{run.id}/retry',headers=_auth(session['user']))
 assert r.status_code==200
 # 第二轮成功（模拟同一冻结输入下重跑得到结果）
 monkeypatch.setattr('src.worker.analyze',
                     lambda p,t:{'total_log_likelihood':-7.25,'trace_count':1})
 s=db_config.SessionLocal();execute_attempt(s,run.id,2);s.close()
 done=client.get(f'/runs/{run.id}',headers=_auth(session['user'])).json()
 assert done['status']=='completed'
 assert done['attempt_no']==2
 assert done['total_log_likelihood']==-7.25
 # 每轮记录齐全
 attempts={h['attempt'] for h in done['attempt_history']}
 assert attempts=={1,2}
 assert any(h['phase']=='failed' and h['attempt']==1 for h in done['attempt_history'])
 assert any(h['phase']=='retry' and h['attempt']==2 for h in done['attempt_history'])
 assert any(h['phase']=='completed' and h['attempt']==2 for h in done['attempt_history'])
