import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Ezequiel Lima",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 10)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def titanic_dag_1():

    inicio = DummyOperator(task_id="inicio")

    @task
    def ingestao(url):
        LOCAL_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(url, sep=';')
        df.to_csv(LOCAL_ARQUIVO, index=False, sep=";")
        return LOCAL_ARQUIVO

    @task
    def ind_passageiros_1(nome_do_arquivo):
        LOCAL_ARQUIVO = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"PassengerId": "count"}).reset_index().rename(columns={'PassengerId': 'Passengers'})
        print(res)
        res.to_csv(LOCAL_ARQUIVO, index=False, sep=";")
        return LOCAL_ARQUIVO

    @task
    def ind_passageiros_2(nome_do_arquivo):
        LOCAL_ARQUIVO = "/tmp/preco_medio_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {'Fare': 'mean'}).reset_index()
        print(res)
        res.to_csv(LOCAL_ARQUIVO, index=False, sep=";")
        return LOCAL_ARQUIVO

    @task
    def ind_passageiros_3(nome_do_arquivo):
        LOCAL_ARQUIVO = "/tmp/familiares_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['SibSp_Parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg(
            {'SibSp_Parch': 'sum'}).reset_index()
        print(res)
        res.to_csv(LOCAL_ARQUIVO, index=False, sep=";")
        return LOCAL_ARQUIVO

    @task
    def ind_final(nome_do_arquivo_1, nome_arquivo_2, nome_do_arquivo_3):
        LOCAL_ARQUIVO = "/tmp/tabela_unica.csv"
        df_ind_1 = pd.read_csv(nome_do_arquivo_1, sep=';')
        df_ind_2 = pd.read_csv(nome_arquivo_2, sep=';')
        df_ind_3 = pd.read_csv(nome_do_arquivo_3, sep=';')
        res = df_ind_1.merge(df_ind_2, how='inner', on=['Sex', 'Pclass'])
        res = res.merge(df_ind_3, how='inner', on=['Sex', 'Pclass'])
        print(res)
        res.to_csv(LOCAL_ARQUIVO, index=False, sep=';')
        return LOCAL_ARQUIVO

    # Orquestrar
    ing = ingestao(URL)
    indicador_1 = ind_passageiros_1(ing)
    indicador_2 = ind_passageiros_2(ing)
    indicador_3 = ind_passageiros_3(ing)
    indicador_final = ind_final(indicador_1, indicador_2, indicador_3)

    triggerdag = TriggerDagRunOperator(
        task_id="executa_dag_2",
        trigger_dag_id="titanic_dag_2"
    )
    fim = DummyOperator(task_id="fim")

    inicio >> ing
    indicador_final >> triggerdag >> fim


execucao = titanic_dag_1()
