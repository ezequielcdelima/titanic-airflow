import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime


NOME_ARQUIVO = '/tmp/tabela_unica.csv'
default_args = {
    'owner': "Ezequiel Lima",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 10)
}


@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['Titanic'])
def titanic_dag_2():

    inicio = DummyOperator(task_id="inicio")

    @task
    def resultado():
        LOCAL_ARQUIVO = '/tmp/resultados.csv'
        df = pd.read_csv(NOME_ARQUIVO, sep=';')
        res = df.agg({'Passengers': 'mean', 'Fare': 'mean', 'SibSp_Parch': 'mean'}).reset_index(
        ).rename(columns={'index': 'Indicator', 0: 'Mean_Value'})
        print(res)
        res.to_csv(LOCAL_ARQUIVO, index=False, sep=';')
        return LOCAL_ARQUIVO

    fim = DummyOperator(task_id="fim")
    res = resultado()

    # Orquestrar
    inicio >> res >> fim


execucao = titanic_dag_2()
