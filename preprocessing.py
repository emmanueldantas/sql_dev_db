import numpy as np
import pandas as pd
import re

default_columns = [
    'key_id_cliente',
    'key_nome_cliente',
    'key_potencia_vendida_w',
    'key_geracao_vendida',
    'key_concessionaria',
    'key_tipo_oferta',
    'key_etapa_atual',
    'key_funil_atual',
    'key_data_ganho',
    'key_data_registro',
    'key_centro_custo'
]

keep_firs_part = lambda x: str(x).split(' ')[0]
find_nan = lambda x: x if re.findall("\d+", str(x)) else np.NaN
fix_nones = lambda x: np.NaN if (str(x) == 'None' or str(x) == 'NaN') else x
get_cc = lambda x: x[:4] if re.findall("\d+", str(x)) else np.NaN
to_wp = lambda x: float(x) * 1000 if re.findall("\d+", str(x)) else np.NaN
filter_name = lambda x: x if x != 'null' else np.NaN
filter_numbers = lambda x: x if isinstance(x, float) else '.'.join(re.findall("\d+", str(x)))
clear_dots = lambda x: x if isinstance(x, float) else str(x).replace('.', '')


def convert_to_float(x):
    x = str(x)
    if ',' in x and '.' in x:
        x = x.replace('.', '').replace(',', '.')

    elif ',' in x:
        x = x.replace(',', '.')
    try:
        return float(x)
    except:
        return np.NaN


def to_dataframe(column_names, data_table):
    dt = {column: data for column, data in zip(column_names, data_table)}
    return pd.DataFrame(dt)


def create_missing_columns(df):
    for column in default_columns:
        if column not in df.columns:
            df[column] = np.NaN

    return df.copy()


def keep_best_duplicated_rows(df, subset):
    df['sort'] = df.isnull().sum(axis=1)
    df = df.sort_values(by=['sort'])
    df.drop('sort', axis=1, inplace=True)
    df.drop_duplicates(keep='first', inplace=True, subset=subset)
    return df.copy()


def clean_data_plataforma(dt):
    columns=[
        'id_plataforma',
        'key_id_cliente',
        'key_nome_cliente',
        'key_geracao_vendida',
        'key_potencia_vendida_w',
        'key_concessionaria',
        'key_tipo_oferta',
        'key_etapa_atual',
        'key_funil_atual',
        'key_data_ganho'
    ]

    df = to_dataframe(columns, dt)

    df = keep_best_duplicated_rows(df, subset=['key_id_cliente'])
    df = df.applymap(fix_nones)

    df['key_nome_cliente'] = df['key_nome_cliente'].str.replace("'", '').replace('"', "")
    df['key_potencia_vendida_w'] = df['key_potencia_vendida_w'].apply(to_wp)
    df['key_geracao_vendida'] = df['key_geracao_vendida'].astype(float)
    df['key_id_cliente'] = df['key_id_cliente'].fillna(df['id_plataforma'])
    df['key_id_cliente'] = df['key_id_cliente'].astype(int)
    df = df[df['key_id_cliente'] > 0]
    df = df.drop(columns=['id_plataforma'])
    
    df = create_missing_columns(df)   
    return df.copy()


def clean_data_pipedrive(dt):
    columns=[
        'key_id_cliente',
        'key_potencia_vendida_w',
        'key_centro_custo',
        'key_geracao_vendida'
    ]

    df = to_dataframe(columns, dt)
    df = keep_best_duplicated_rows(df, subset=['key_id_cliente'])
    df = df.applymap(fix_nones)

    df['key_id_cliente'] = df['key_id_cliente'].astype(int)
    df['key_potencia_vendida_w'] = df['key_potencia_vendida_w'].apply(to_wp)
    df['key_centro_custo'] = df['key_centro_custo'].apply(get_cc)
    df['key_geracao_vendida'] = df['key_geracao_vendida'].apply(find_nan)
    df['key_geracao_vendida'] = df['key_geracao_vendida'].apply(keep_firs_part)
    df['key_geracao_vendida'] = df['key_geracao_vendida'].apply(convert_to_float)

    df = create_missing_columns(df)  
    return df.copy()


def merge_bases(main_df, secondary_df, on='key_id_cliente'):
    main_df = main_df.set_index(on) 
    secondary_df = secondary_df.set_index(on) 
    secondary_df.update(main_df)
    secondary_df = secondary_df.reset_index(level=0)
    return secondary_df.replace(np.NaN, None).copy()
