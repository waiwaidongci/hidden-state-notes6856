"""跨连接并发：两个独立连接（模拟两个 worker 进程）重复投递同一轮，只执行一次。"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config import database as db_config
from src.config.database import Base
from src.models import InferenceRun,SequenceStudy,ModelProfile,ObservationTrace
from src.worker import execute_attempt
from src.services.runs import retry_run
from src.utils.serialize import row
@pytest.fixture
def file_db(tmp_path):
 """基于文件的 SQLite，支持多连接真实并发访问，替换全局 SessionLocal。"""
 path=tmp_path/'contention.db'
 engine=create_engine(f'sqlite:///{path}',connect_args={'timeout':15})
 maker=sessionmaker(engine,expire_on_commit=False)
 old_engine,old_session=db_config.engine,db_config.SessionLocal
 db_config.engine=engine;db_config.SessionLocal=maker
 Base.metadata.create_all(engine)
 yield maker,engine
 db_config.engine=old_engine;db_config.SessionLocal=old_session
 engine.dispose()
@pytest.fixture
def catalog(file_db):
 maker,engine=file_db
 db=maker()
 study=SequenceStudy(name='并发研究',filename='f.json',description='',symbols=['A','B','C'])
 db.add(study);db.flush()
 profile=ModelProfile(study_id=study.id,name='并发-v1',version='v1',
                      states=['S0','S1'],symbols=['A','B','C'],created_by='a@x',
                      parameters={'pi':[1.0,0.0],'transition':[[1.0,0.0],[0.5,0.5]],
                                  'emission':[[0.5,0.5,0.0],[0.0,0.0,1.0]]},
                      normalization_max_delta=0.0)
 db.add(profile);db.flush()
 ok=ObservationTrace(study_id=study.id,reference='T-OK',tokens=['A','B','A'])
 bad=ObservationTrace(study_id=study.id,reference='T-BAD',tokens=['C'])
 db.add_all([ok,bad]);db.commit()
 ctx={'maker':maker,'study_id':study.id,'profile_id':profile.id,
      'profile_row':row(profile),'ok_row':row(ok),'bad_row':row(bad)}
 db.close()
 return ctx
def _make_run(maker,ctx,trace_key,name):
 db=maker()
 run=InferenceRun(study_id=ctx['study_id'],profile_id=ctx['profile_id'],name=name,
                  profile_snapshot=ctx['profile_row'],
                  trace_snapshot=[ctx[trace_key]],
                  created_by='a@x',attempt_no=1,
                  attempt_history=[{'attempt':1,'phase':'queued','status':'queued',
                                    'at':'2026-11-12T09:00:00+00:00','error':None}])
 db.add(run);db.commit();run_id=run.id;db.close()
 return run_id
def test_duplicate_delivery_across_connections_runs_once(catalog,monkeypatch):
 maker=catalog['maker']
 run_id=_make_run(maker,catalog,'ok_row','并发-可成功')
 calls={'n':0}
 from src.services import analysis
 real=analysis.analyze
 def counting(profile,traces):
  calls['n']+=1
  return real(profile,traces)
 monkeypatch.setattr('src.worker.analyze',counting)
 # 两个独立连接同时投递同一轮（attempt 1，queued 状态）
 s1,s2=maker(),maker()
 execute_attempt(s1,run_id,1)
 execute_attempt(s2,run_id,1)
 s1.close();s2.close()
 check=maker();saved=check.get(InferenceRun,run_id);check.close()
 assert saved.status=='completed'
 assert calls['n']==1
 assert len([h for h in saved.attempt_history if h['phase']=='completed'])==1
def test_cross_connection_stale_attempt_is_ignored(catalog,monkeypatch):
 maker=catalog['maker']
 run_id=_make_run(maker,catalog,'bad_row','并发-先失败')
 s=maker();execute_attempt(s,run_id,1);s.close()
 d=maker();retry_run(d,run_id,'b@x');d.close()
 # 旧轮次（1）在新一轮已排队后才到达：必须被忽略
 monkeypatch.setattr('src.worker.analyze',
   lambda p,t:{'total_log_likelihood':-99.0,'trace_count':1})
 stale=maker();execute_attempt(stale,run_id,1);stale.close()
 check=maker();saved=check.get(InferenceRun,run_id);check.close()
 assert saved.attempt_no==2 and saved.status=='queued'
 assert saved.result is None
 # 新一轮执行成功，结果不被旧执行影响
 cur=maker();execute_attempt(cur,run_id,2);cur.close()
 check=maker();saved=check.get(InferenceRun,run_id);check.close()
 assert saved.status=='completed' and saved.attempt_no==2
 phases={(h['attempt'],h['phase']) for h in saved.attempt_history}
 assert (1,'failed') in phases and (2,'retry') in phases and (2,'completed') in phases
