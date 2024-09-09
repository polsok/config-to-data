import yaml
import csv
import fnmatch
import shutil
import pandas as pd
import os
import os.path
from collections import Counter
class Conf:
    # 1 чистим папку с выводом
    def clearFolder(path):
        # Проверяем, существует ли каталог
        if os.path.exists(path):
            # Удаляем каталог и его содержимое
            shutil.rmtree(path)
        # Создаем новый пустой каталог
        os.mkdir(path)
    # загружаем статистику и на вывод получаем список файлов в заданной директории
    def findYamlFiles(directory):
        # обходим нужную папку выводим список всех файлов и список уникальных файлов
        files = []
        uniq = []
        for root, dirs, files_ in os.walk(directory):
            for file_ in files_:
                # Вырезаем общую часть строки
                output_string = os.path.join(root, file_).replace(directory + "/", "")
                # проверяем что ссылка на файл корректна
                index = output_string.find('/')
                if index != -1:
                    uniq.append(os.path.join(file_))
                    files.append(os.path.join(root, file_))
            # Используем Counter для подсчета количества каждого элемента
        counter = Counter(uniq)
        sorted_counts = sorted(counter.items(), key=lambda x: x[1], reverse=True)
        # Выводим результаты
        for key, value in sorted_counts:
            with open("stats/counter.txt", "a") as file:
                file.write(f"{key}, {value}\n")
        uniq = sorted(set(uniq))
        for name_file in uniq:
            with open("stats/name.txt", "a") as file:
                file.write(f"{name_file}\n")
        return files
    def check_rules(directory, files, rules):
        datas = []
        for file_ in files:
            # Вырезаем общую часть строки
            output_string = file_.replace(directory, "")
            # проверка, что первый символ косая черта
            if output_string[0] == '/':
                output_string = output_string[1:]
            # обработка правила conf
            if any(value == 'kind' for value in rules.values()):
                with open("stats/files.txt", "a") as file:
                    file.write(f"{output_string}\n")
                if file_.endswith('.yaml' or '.yml'):
                    try:
                        with open(os.path.join(file_), 'r') as f:
                            yaml_data = yaml.safe_load(f)
                            if 'kind' in yaml_data and any(fnmatch.fnmatch(yaml_data["kind"], rule) for rule in rules):
                                # отдельно сохраняем расширение, отдельно полное имя, имя сервиса и полный путь
                                datas.append({
                                    'full_path': os.path.join(file_),
                                    'short_path': output_string,
                                    'service_name': output_string.split('/')[0],
                                    'name_conf' : output_string.split('/')[-1],
                                    'file_extension': output_string.split('/')[-1].split('.')[-1],
                                    'kind': yaml_data['kind']
                                })
                    except yaml.YAMLError as e:
                        print(f"ERROR {os.path.join(file_)}")
        for _file in datas:
            with open("stats/data.txt", "a") as file:
                file.write(f"{_file}\n")
        return datas
    # на вход подаем список файлов, на выход получаем список уникальных имен файлов, в каждом из которых содержится список файлов с полными путями?
    def getListNameFiles(files):
        # получаем список уникальных файлов
        string_set = set()
        for file in files:
            if file['kind'] not in string_set:
                string_set.add(file['kind'])
        return list(string_set)
    # группируем объекты по kind
    def group_objects_by_kind(files):
        grouped_objects = {}
        for obj in files:
            kind = obj['kind']
            # if kind == "Service":
            if kind not in grouped_objects:
                grouped_objects[kind] = []
            grouped_objects[kind].append(obj)
        # Возвращаем словарь с сгруппированными объектами
        return grouped_objects
    def handler(path):
        # Словарь для хранения данных
        data = {}
        # Открываем CSV файл для чтения
        with open(path, 'r', newline='') as csvfile:
            # Создаем объект reader для чтения CSV файла
            reader = csv.reader(csvfile)
            # Пропускаем заголовок, если он есть
            next(reader)
            # Читаем каждую строку CSV файла
            for row in reader:
                # Первый элемент строки - ключ, второй - значение
                data[row[0]] = row[1]
        # Закрываем CSV файл
        csvfile.close()
        # Выводим словарь
        print(data)
        return data
    # метод обработки yaml данных
    def GetData(data, prefix):
        if isinstance(data, dict):
            for k, v in data.items():
                yield from Conf.GetData(v, f'{prefix}/{k}')
        elif isinstance(data, list):
            for i, v in enumerate(data):
                yield from Conf.GetData(v, f'{prefix}/{i}')
        else:
            yield (prefix, data)
    # метод обработки yaml файлов
    def GetYamlData(file: str):
        with open(file, "r") as f:
            yaml_data = yaml.safe_load(f)
            retval = pd.DataFrame(Conf.GetData(yaml_data, '')).set_index(0)
            output_string = file.replace("/Users/19901341/.osdf/buildConfigs/", "")
            retval = retval.rename(columns={1:output_string.split('/')[0]})
        return(retval)
    def get_data(grouped_objects, uniq):
        for kind, objects in grouped_objects.items():
            # lines.add(f"сервис, количество ")
            if kind in uniq:
                df = pd.concat([Conf.GetYamlData(object['full_path']) for object in objects], axis=1)
                a = df.keys
                # df.iloc[0, 0] = "ID"
                df.to_csv(f'results/{kind}.csv', index=True)
                # получаем список неизменяемых ключей (ну и сразу их выводим в файлик)
                unmutable_data = df[df.iloc[:, 1:].eq(df.iloc[:,1], axis=0).all(1)].index
