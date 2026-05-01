import os
import yaml
import oracledb
import sqlparse

def format_plsql(raw_ddl):
    # Приводим к единому виду: ключевые слова в верхний регистр, 
    # фиксированные отступы в 4 пробела
    formatted = sqlparse.format(
        raw_ddl,
        reindent=True,
        keyword_case='upper',
        indent_width=4,
        strip_comments=False # лучше оставить для Git
    )
    return formatted

def get_connection(db_cfg):
    # Указываем путь к Wallet и TNS (обычно это одна папка с файлами из архива)
    wallet_path = db_cfg.get('wallet_path')
    
    try:
        if db_cfg['method'] == 'tns':
            # Для TNS нам нужно сказать драйверу, где искать tnsnames.ora и wallet
            conn = oracledb.connect(
                user=db_cfg['user'],
                password=db_cfg['password'],
                dsn=db_cfg['tns_name'],
                config_dir=wallet_path,     # Путь к tnsnames.ora
                wallet_location=wallet_path, # Путь к cwallet.sso
                wallet_password=db_cfg.get('wallet_password') # Если кошелек зашифрован
            )
        else:
            # Обычное подключение через Service Name
            dsn = f"{db_cfg['host']}:{db_cfg['port']}/{db_cfg['service_name']}"
            conn = oracledb.connect(
                user=db_cfg['user'], 
                password=db_cfg['password'], 
                dsn=dsn
            )
        return conn
    except oracledb.Error as e:
        print(f"Ошибка подключения: {e}")
        return None

def run_export():
    # 1. Получаем абсолютный путь к директории, где лежит текущий скрипт
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. Соединяем путь к папке с именем файла конфига
    config_path = os.path.join(script_dir, "config.yaml")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Файл config.yaml не найден!")
        return

    # 2. Создание корневой папки экспорта
    root_export_dir = config.get('export_path', '')
    
    # 3. Подключение к Oracle
    db_cfg = config['db_conn']
    try:
        conn = get_connection(db_cfg)
        cursor = conn.cursor()
        
        # Настройка форматирования DDL
        cursor.execute("begin dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', false); end;")
        
        # 4. Основной цикл по пользователям из YAML
        for user_item in config.get('users_to_export', []):
            owner = user_item.upper()
            obj_types = config.get('types', [])
            
            print(f"--- Обработка пользователя: {owner} ---")
            user_dir = os.path.join(root_export_dir, owner)
            
            for obj_type in obj_types:
                obj_type = obj_type.upper()
                
                # Получаем список имен объектов этого типа для данного owner
                cursor.execute(
                    "SELECT object_name FROM all_objects WHERE owner = :1 AND object_type = :2",
                    [owner, obj_type]
                )
                objects = cursor.fetchall()

                if objects:
                    # Создаем папку типа (например, TABLES) только если объекты найдены
                    type_dir = os.path.join(user_dir, obj_type)
                    os.makedirs(type_dir, exist_ok=True)
                    
                    for (obj_name,) in objects:
                        try:
                            # Получаем DDL
                            cursor.execute(f"SELECT dbms_metadata.get_ddl(:1, :2, :3) FROM dual", [obj_type, obj_name, owner])
                            ddl_lob = cursor.fetchone()[0]
                            ddl_text = ddl_lob.read() if ddl_lob else ""
                            # clean_ddl = format_plsql(ddl_text)

                            # Сохраняем в файл
                            file_path = os.path.join(type_dir, f"{obj_name}.sql")
                            with open(file_path, "w", encoding="utf-8") as f:
                                f.write(ddl_text)
                            print(f"  [OK] {obj_type}: {obj_name}")
                        except Exception as e:
                            print(f"  [Ошибка] Не удалось получить DDL для {obj_name}: {e}")
                else:
                    print(f"  [Инфо] Объекты типа {obj_type} для {owner} не найдены.")

    except oracledb.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_export()
