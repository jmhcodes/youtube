# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import range
from datetime import timedelta
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date':datetime.now() ,
}

dag = DAG(
    dag_id='pull_videos_stats',
    default_args=args,
    schedule_interval='0 0,2,4,6,8,10,12,14,16,18,20,22 * * *',
    dagrun_timeout=timedelta(seconds=5),
    tags=['example'], 
    catchup=False
)

t1 = BashOperator(
        task_id = 'testairflow',
        bash_command='python /home/blood_dragon/git_proj/youtube/video_stats_daily.py', 
        dag=dag
        )
