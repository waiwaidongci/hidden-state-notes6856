import os
import sys
from pathlib import Path
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
BACKEND=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BACKEND))
# 保证 tests 作为单一包加载，避免 conftest / tests.conftest 双模块导致类身份不一致
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('REDIS_URL','redis://localhost:6379/0')
os.environ.setdefault('ARTIFACT_DIR','/tmp/hmm-artifacts-test')
os.environ.setdefault('SEED_DEMO','false')
import src.models  # noqa: F401  # 导入即注册全部 ORM 模型到 Base.metadata
from src.config import database as db_config  # noqa: E402
from src.models import (SequenceStudy,ModelProfile,ObservationTrace,  # noqa: E402
                        InferenceRun,User)
from src.utils.security import issue_token  # noqa: E402
VALID_PROFILE={
 'states':['假设甲','假设乙'],
 'symbols':['A','B','C'],
 'parameters':{'pi':[0.6,0.4],
               'transition':[[0.7,0.3],[0.4,0.6]],
               'emission':[[0.5,0.4,0.1],[0.1,0.3,0.6]]},
 'normalization_max_delta':0.0,
}
@pytest.fixture
def session():
 engine=create_engine('sqlite://',connect_args={'check_same_thread':False},
                      poolclass=StaticPool)
 db_config.engine=engine
 db_config.SessionLocal=sessionmaker(engine,expire_on_commit=False)
 db_config.Base.metadata.create_all(engine)
 db=db_config.SessionLocal()
 study=SequenceStudy(name='A研究',filename='yx-fixed-hmm-1-202611.json',
                     description='d',symbols=['A','B','C'])
 db.add(study);db.flush()
 profile=ModelProfile(study_id=study.id,name='YX1-固定参数-v1',version='v1',
                      states=VALID_PROFILE['states'],symbols=VALID_PROFILE['symbols'],
                      parameters=VALID_PROFILE['parameters'],
                      normalization_max_delta=0.0,created_by='admin@yanxi.demo')
 db.add(profile);db.flush()
 ok_trace=ObservationTrace(study_id=study.id,reference='YX1-T001',
                           tokens=['A','B','C','A','B'])
 # 初态不可能发射 C（pi[1] 发射 C=.6 可行；用 B 发射结构制造不可能：两状态首码都不发射的码不存在，
 # 改用受限模型见 impossible_profile 夹具）
 bad_profile=ModelProfile(study_id=study.id,name='YX3-受限发射-v1',version='v1',
                          states=['S0','S1'],symbols=['A','B','C'],
                          parameters={'pi':[1.0,0.0],
                                      'transition':[[1.0,0.0],[0.5,0.5]],
                                      'emission':[[0.5,0.5,0.0],[0.0,0.0,1.0]]},
                          normalization_max_delta=0.0,created_by='admin@yanxi.demo')
 db.add(bad_profile);db.flush()
 bad_trace=ObservationTrace(study_id=study.id,reference='YX3-T009',tokens=['C'])
 db.add_all([ok_trace,bad_trace]);db.commit()
 user=User(email='analyst@yanxi.demo',name='许汐',role='analyst',
           password_hash='x')
 db.add(user);db.commit()
 def new_session():return db_config.SessionLocal()
 yield {'db':db,'new_session':new_session,'study':study,'profile':profile,
        'bad_profile':bad_profile,'ok_trace':ok_trace,'bad_trace':bad_trace,
        'user':user}
 db.close();engine.dispose()
@pytest.fixture
def token(session):
 return issue_token(session['user'].id)
@pytest.fixture(autouse=True)
def fake_broker(monkeypatch):
 """默认不触碰真实 Redis/Celery：入队只记录 (run_id, attempt_no)。"""
 enqueued=[]
 class FakeTask:
  def delay(self,run_id,attempt_no=1):enqueued.append((run_id,attempt_no))
 import src.services.runs as runs_service
 monkeypatch.setattr(runs_service,'analyze_run',FakeTask())
 return enqueued
def make_run(session,profile=None,traces=None,name='run-1'):
 db=session['db']
 profile=profile or session['profile']
 traces=traces if traces is not None else [session['ok_trace']]
 from src.utils.serialize import row
 run=InferenceRun(study_id=session['study'].id,profile_id=profile.id,name=name,
                  profile_snapshot=row(profile),
                  trace_snapshot=[row(t) for t in traces],
                  created_by='analyst@yanxi.demo',attempt_no=1,
                  attempt_history=[{'attempt':1,'phase':'queued','status':'queued',
                                    'at':'2026-11-12T09:00:00+00:00','error':None}])
 db.add(run);db.commit()
 db.expunge(run)
 return run
