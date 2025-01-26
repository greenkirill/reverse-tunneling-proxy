import os
import shutil
from filecmp import cmp
from datetime import datetime

# Логирование
def setup_logger():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"sync_log_{timestamp}.txt")
    return log_file

def write_log(log_file, message):
    with open(log_file, "a") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

# Загрузка игнорируемых файлов
def load_ignore_file(ignore_file_path):
    ignore_list = set()
    if os.path.exists(ignore_file_path):
        with open(ignore_file_path, "r") as f:
            for line in f:
                item = line.strip()
                if item:
                    ignore_list.add(item)
    return ignore_list

# Проверка игнорирования
def should_ignore(item_path, ignore_list, base_path):
    rel_path = os.path.relpath(item_path, base_path).replace("\\", "/")
    return any(rel_path.startswith(ignore) for ignore in ignore_list)

# Синхронизация папок
def sync_folders(base, src, dst, ignore_list, log_file):
    if not os.path.exists(dst):
        os.makedirs(dst)
        write_log(log_file, f"Directory created: {dst}")

    src_items = set(os.listdir(src))
    dst_items = set(os.listdir(dst))

    # Удаление лишних файлов/папок
    for item in dst_items - src_items:
        item_path = os.path.join(dst, item)
        if not should_ignore(item_path, ignore_list, dst):
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.remove(item_path)
                write_log(log_file, f"Deleted file: {item_path}")
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                write_log(log_file, f"Deleted folder: {item_path}")
        else:
            write_log(log_file, f"Ignored during deletion: {item_path}")

    # Добавление или обновление файлов/папок
    for item in src_items:
        src_item_path = os.path.join(src, item)
        dst_item_path = os.path.join(dst, item)

        if should_ignore(src_item_path, ignore_list, base):
            write_log(log_file, f"Ignored: {src_item_path}")
            continue

        if os.path.isdir(src_item_path):
            sync_folders(base, src_item_path, dst_item_path, ignore_list, log_file)
        else:
            if not os.path.exists(dst_item_path):
                shutil.copy2(src_item_path, dst_item_path)
                write_log(log_file, f"Copied file: {src_item_path} -> {dst_item_path}")
            elif not cmp(src_item_path, dst_item_path, shallow=False):
                shutil.copy2(src_item_path, dst_item_path)
                write_log(log_file, f"Updated file: {src_item_path} -> {dst_item_path}")
            else:
                write_log(log_file, f"Skipped unchanged file: {src_item_path}")

# Настройка путей и запуск
source_folder = "E:\\mc-servers\\minecolonies-server"
destination_folder = "E:\\mc-servers\\sync-server"
ignore_file = "ignore-files.txt"

log_file = setup_logger()
ignore_list = load_ignore_file(ignore_file)

write_log(log_file, f"Synchronization started: {source_folder} -> {destination_folder}")
sync_folders(source_folder, source_folder, destination_folder, ignore_list, log_file)
write_log(log_file, "Synchronization completed.")
