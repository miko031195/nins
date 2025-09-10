import os
import time
import socket
import xml.etree.ElementTree as ET
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


# Konfiqurasiya
SEND_DELAY_SECONDS = 0.5  # göndərişlər arasında fasilə (saniyə)

# Fayl sabitliyini yoxlamaq üçün parametrlər (yarımçıq yazılan XML-lərə qarşı)
FILE_MIN_AGE_SECONDS = 0.8
FILE_STABILITY_CHECK_INTERVAL = 0.25
FILE_STABILITY_REQUIRED_CHECKS = 2
FILE_STABILITY_TIMEOUT_SECONDS = 15.0


# Maşın qovluqları və ID-lərini təyin etmək
MACHINE_CONFIGS = {
    "masin1": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123321",
        "nvr_ip": "192.168.1.2",
        "nvr_port": 10010,
        "sn": "PN123321",
        "processed": set(),
    },
    "masin2": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123322",
        "nvr_ip": "192.168.1.2",
        "nvr_port": 10011,
        "fallback_port": 10010,
        "sn": "PN123322",
        "processed": set(),
    },
    "masin3": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123323",
        "nvr_ip": "192.168.1.3",
        "nvr_port": 10012,
        "sn": "PN123323",
        "processed": set(),
    },
    "masin4": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123324",
        "nvr_ip": "192.168.1.4",
        "nvr_port": 10013,
        "sn": "PN123324",
        "processed": set(),
    },
    "masin5": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123325",
        "nvr_ip": "192.168.1.5",
        "nvr_port": 10014,
        "sn": "PN123325",
        "processed": set(),
    },
    "masin6": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123326",
        "nvr_ip": "192.168.1.6",
        "nvr_port": 10015,
        "sn": "PN123326",
        "processed": set(),
    },
    "masin7": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123327",
        "nvr_ip": "192.168.1.7",
        "nvr_port": 10016,
        "sn": "PN123327",
        "processed": set(),
    },
    "masin8": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123328",
        "nvr_ip": "192.168.1.8",
        "nvr_port": 10017,
        "sn": "PN123328",
        "processed": set(),
    },
    "masin9": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123329",
        "nvr_ip": "192.168.1.9",
        "nvr_port": 10018,
        "sn": "PN123329",
        "processed": set(),
    },
    "masin10": {
        "path": r"D:\\PronoteFTP-UploadFolder\\Pronote1\\PN123321\\PN123330",
        "nvr_ip": "192.168.1.10",
        "nvr_port": 10019,
        "sn": "PN123330",
        "processed": set(),
    },
}


def _safe_attr(element, attribute_name, default_value=""):
    if element is None:
        return default_value
    return element.attrib.get(attribute_name, default_value)


def _parse_decimal(number_text: str, default_value: Decimal = Decimal("0")) -> Decimal:
    if number_text is None:
        return default_value
    text = str(number_text).strip()
    if not text:
        return default_value
    # Lokal ayırıcı ehtimalını nəzərə alırıq ("," -> ".")
    normalized = text.replace(",", ".")
    try:
        value = Decimal(normalized)
        # Pul dəyərlərini 0.01-ə qədər yuvarlaqlaşdırırıq
        return value.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return default_value


def _wait_for_stable_file(file_path: str) -> bool:
    """
    Faylın ölçüsü və mtime dəyərləri bir neçə yoxlamada dəyişmirsə, faylı sabit sayırıq.
    """
    start_time = time.time()
    last_size = -1
    last_mtime = -1
    stable_checks = 0

    while True:
        try:
            stat = os.stat(file_path)
            size = stat.st_size
            mtime = stat.st_mtime
        except FileNotFoundError:
            if time.time() - start_time > FILE_STABILITY_TIMEOUT_SECONDS:
                return False
            time.sleep(FILE_STABILITY_CHECK_INTERVAL)
            continue

        age_seconds = time.time() - mtime
        if size == last_size and mtime == last_mtime and age_seconds >= FILE_MIN_AGE_SECONDS:
            stable_checks += 1
        else:
            stable_checks = 0

        if stable_checks >= FILE_STABILITY_REQUIRED_CHECKS:
            return True

        if time.time() - start_time > FILE_STABILITY_TIMEOUT_SECONDS:
            return False

        last_size = size
        last_mtime = mtime
        time.sleep(FILE_STABILITY_CHECK_INTERVAL)


def process_xml(file_path, machine_id):
    try:
        # Yarim cəkilmiş faylları gözləyirik
        _wait_for_stable_file(file_path)

        tree = ET.parse(file_path)
        root = tree.getroot()

        deposit_node = root.find(".//Deposit")
        machine_node = root.find(".//Machine")

        # Təhlükəsiz atribut oxunuşu (None olarsa default)
        deposit_end_time = _safe_attr(deposit_node, "DepositEndDateTime", "")
        machine_sn = _safe_attr(machine_node, "MachineSN", "")
        currency = _safe_attr(deposit_node, "Currency", "")

        denom_list = []
        total_amount = Decimal("0.00")
        total_banknotes = 0

        for bn in root.findall(".//BN"):
            denom_text = bn.attrib.get("Denom", "0")
            denom_decimal = _parse_decimal(denom_text, Decimal("0"))
            # Nominalları saymaq üçün tam ədədə çeviririk (məs: 50.0 -> 50)
            denom_int = int(denom_decimal.to_integral_value(rounding=ROUND_HALF_UP))
            denom_list.append(denom_int)
            total_amount += denom_decimal
            total_banknotes += 1

        denom_counts = Counter(denom_list)
        sorted_denoms = sorted(denom_counts.items())

        formatted_nominals = [f"{denom}({count} Ədəd)" for denom, count in sorted_denoms]
        nominals_text = ", ".join(formatted_nominals)

        total_amount_str = f"{total_amount:.2f}"

        message = (
            f"{machine_id}\n"
            f"SN:{machine_sn}\n"
            f"Saat:{deposit_end_time}\n"
            f"Valyuta:{currency}\n"
            f"Umumi_Mebleg:{total_amount_str}\n"
            f"Umumi_Say:{total_banknotes}\n"
            f"Nominallar:{nominals_text}"
        )

        print(f"Gonderilen melumatlari: {message}")
        print(f"Fayl: {os.path.basename(file_path)}")
        print("-" * 80)

        send_to_nvr(message, machine_id)

    except ET.ParseError as e:
        print(f"Xeta (XML parse) - Fayl: {file_path}, Sebeb: {e}")
    except Exception as e:
        print(f"Xeta - Fayl: {file_path}, Sebeb: {e}")


def send_to_nvr(message, machine_id):
    config = MACHINE_CONFIGS[machine_id]
    nvr_ip = config["nvr_ip"]
    primary_port = config["nvr_port"]
    fallback_port = config.get("fallback_port")

    success = try_send_to_port(message, nvr_ip, primary_port)

    if not success and fallback_port:
        time.sleep(0.5)
        success = try_send_to_port(message, nvr_ip, fallback_port)

    if not success:
        # ən azından log yazırıq ki, diaqnostika mümkün olsun
        print(f"Xeberdarliq: NVR qoşulması alınmadı ({nvr_ip}:{primary_port} / {fallback_port or 'fallback yoxdur'})")
        time.sleep(0.5)


def try_send_to_port(message, nvr_ip, port):
    try:
        with socket.create_connection((nvr_ip, port), timeout=0.5) as s:
            s.sendall(message.encode("utf-8"))
            print(f"NVR-e ugurla gonderildi ({nvr_ip}:{port})")
            time.sleep(SEND_DELAY_SECONDS)
            return True
    except Exception:
        return False


def _list_xml_entries_sorted_by_mtime(directory_path: str):
    try:
        entries = []
        with os.scandir(directory_path) as it:
            for entry in it:
                if entry.is_file() and entry.name.lower().endswith(".xml"):
                    try:
                        stat = entry.stat()
                        entries.append((entry.name, stat.st_mtime))
                    except FileNotFoundError:
                        # Fayl arada silinibsə, keçirik
                        continue
        entries.sort(key=lambda pair: pair[1])
        return [name for name, _ in entries]
    except FileNotFoundError:
        return []


def _cleanup_processed_set(processed_paths: set, directory_path: str):
    """Qovluqda artıq mövcud olmayan faylları processed-dən silirik (yaddaşın böyüməməsi üçün)."""
    try:
        existing = set()
        with os.scandir(directory_path) as it:
            for entry in it:
                if entry.is_file() and entry.name.lower().endswith(".xml"):
                    existing.add(os.path.join(directory_path, entry.name))
        processed_paths.intersection_update(existing)
    except FileNotFoundError:
        processed_paths.clear()


def main():
    print("Masin monitorinqi basladi...")
    print("Izlenen masinlar:")

    for machine_id, config in MACHINE_CONFIGS.items():
        fallback_info = f" -> Fallback: {config.get('fallback_port', 'Yoxdur')}" if config.get("fallback_port") else ""
        print(f"  - {machine_id}: {config['path']} -> {config['nvr_ip']}:{config['nvr_port']}{fallback_info}")
    print("-" * 80)

    # Başlangıcda mövcud faylları processed olaraq işarələyirik
    print("Movcud xml faylları boş buraxılır...")
    for machine_id, config in MACHINE_CONFIGS.items():
        try:
            watch_dir = config["path"]
            processed_files = config["processed"]

            if os.path.exists(watch_dir):
                existing_files = [f for f in _list_xml_entries_sorted_by_mtime(watch_dir)]
                for file in existing_files:
                    file_path = os.path.join(watch_dir, file)
                    processed_files.add(file_path)
                print(f"{machine_id}: {len(existing_files)} movcud fayl boş buraxılır")
            else:
                print(f"Xeta: {machine_id} qovlugu tapilmadi: {watch_dir}")
        except Exception as e:
            print(f"Başlanğıc yoxlama xetasi [{machine_id}]: {e}")

    print("Yeni fayllar uzre monitorinq basladir...")
    print("-" * 80)

    while True:
        for machine_id, config in MACHINE_CONFIGS.items():
            try:
                watch_dir = config["path"]
                processed_files = config["processed"]

                if not os.path.exists(watch_dir):
                    continue

                # processed dəstini təmizləyirik ki, yaddaş artmasın
                _cleanup_processed_set(processed_files, watch_dir)

                files = _list_xml_entries_sorted_by_mtime(watch_dir)

                for file in files:
                    file_path = os.path.join(watch_dir, file)
                    if file_path not in processed_files:
                        print(f"Yeni fayl tapildi [{machine_id}]: {file}")
                        process_xml(file_path, machine_id)
                        processed_files.add(file_path)

            except Exception as e:
                print(f"Qovluq monitorinqi xetasi [{machine_id}]: {e}")

        time.sleep(0.5)


if __name__ == "__main__":
    main()

