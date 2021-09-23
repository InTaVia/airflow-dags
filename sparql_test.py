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

"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_python_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    def query_sparql():
        from SPARQLWrapper import SPARQLWrapper, JSON
        sparql = SPARQLWrapper("https://intavia-ts.acdh-dev.oeaw.ac.at/sparql")

        query = """
            PREFIX dct: <http://purl.org/dc/terms/>

                SELECT ?g ?cnt ?created ?creator WHERE { {SELECT DISTINCT ?g (COUNT(DISTINCT ?s) as ?cnt) WHERE { GRAPH ?g { ?s ?p ?o . } } GROUP BY ?g }
                OPTIONAL {?g dct:created ?created; dct:creator ?creator; }
                }        
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        pprint(results)

    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=query_sparql,
        requirements=["SPARQLWrapper==1.8.5"],
        system_site_packages=False,
    )
    # [END howto_operator_python_venv]