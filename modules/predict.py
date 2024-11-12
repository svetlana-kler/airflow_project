import os
import logging
from datetime import datetime

import dill
import json
import pandas as pd
from glob import glob

# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '.')
logging.basicConfig(level=logging.INFO)

logging.info(os.path.abspath(path))         # стартовый путь
path_to_models = f'{path}/data/models'      # путь до моделей
logging.info(os.path.abspath(path_to_models))
path_to_test = f'{path}/data/test'          # путь до данных JSON для предсказаний
logging.info(os.path.abspath(path_to_test))
path_to_preds = f'{path}/data/predictions'  # путь до предсказаний
logging.info(os.path.abspath(path_to_preds))


def download_best_model():
    models_list = glob(os.path.join(path_to_models, '*.pkl'))
    latest_model = max(models_list, key=os.path.getmtime)
    with open(latest_model, 'rb') as file:
        model = dill.load(file)

    return model


def generate_predictions():
    model = download_best_model()
    predictions = []                                    # список для хранения предсказаний
    for datapath in glob(os.path.join(path_to_test, '*.json')):  # чтение JSON файлов и выполнение предсказаний
        with open(datapath, 'r') as datafile:
            df = pd.json_normalize(json.load(datafile)) # преобразование JSON в DataFrame

            y = model.predict(df)                       # выполнение предсказания

            predictions.append({
                'car_id': df['id'].tolist(),
                'pred': y.tolist()
            })                                          # добавление предсказаний в список

    return predictions


def predict():
    preds = generate_predictions()                      # сохранение всех предсказаний в csv файл
    preds_df = pd.DataFrame(preds)
    preds_df.to_csv(os.path.join(path_to_preds,f'preds_{datetime.now().strftime("%Y%m%d%H%M")}.csv'), index=False)

    logging.info(preds_df)


if __name__ == '__main__':
    predict()
