import base64
import hashlib
import os
import re
import ssl
import threading
import queue
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import tkinter as tk
from tkinter import messagebox

import ttkbootstrap as tb
from ttkbootstrap.constants import LEFT, RIGHT, TOP, BOTH, X, Y

from PIL import Image, ImageOps, ImageTk

from serial.tools import list_ports
from escpos.printer import Serial as EscposSerial
from openai import OpenAI

import paho.mqtt.client as mqtt


# =========================
# Defaults
# =========================
DEFAULT_PRINTER_BAUD = 9600
DEFAULT_PRINTER_WIDTH = 529

DEFAULT_MQTT_HOST = "test.mosquitto.org"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TLS = False
DEFAULT_TOPICS = ["mschoolsLleida", "mschoolsTarragona", "mschoolsGirona", "mschoolsBarcelona"]

OPENAI_MODEL = "gpt-image-1"
OPENAI_SIZE = "1024x1024"
OPENAI_FORMAT = "png"

# Desktop folders
DESKTOP_PATH = Path.home() / "Desktop"
DEFAULT_APP_FOLDER = DESKTOP_PATH / "mSchools_Print"
DEFAULT_OUT_DIR = DEFAULT_APP_FOLDER / "images"
DEFAULT_LOG_FILE = DEFAULT_APP_FOLDER / "print_log.tsv"

DEFAULT_APP_FOLDER.mkdir(parents=True, exist_ok=True)
DEFAULT_OUT_DIR.mkdir(parents=True, exist_ok=True)

TEST_IMAGE_PATH = DEFAULT_APP_FOLDER / "test.png"
LOGO_PATH = DEFAULT_APP_FOLDER / "logo_FLU.png"

# UI colors
PURPLE = "#5b1a4a"
GREEN = "#20e070"
BG_WHITE = "#ffffff"


# =========================
# Helpers
# =========================
def get_com_ports() -> list[str]:
    return [p.device for p in list_ports.comports()]


def safe_line(s: str) -> str:
    return s.replace("\r", "").strip()


def slugify_group_prefix(group_name: str) -> str:
    s = (group_name or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "grup"
    return f"{s}_"


def split_group_and_prompt(payload: str) -> tuple[str, str]:
    text = safe_line(payload)
    if "." not in text:
        return "", text
    group, rest = text.split(".", 1)
    return group.strip(), rest.strip()


def build_filename(prefix: str, seed: str, ext: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    h = hashlib.sha256(seed.encode("utf-8", errors="ignore")).hexdigest()[:10]
    prefix = prefix or ""
    return f"{prefix}img_{ts}_{h}.{ext}"


def append_log_tsv(log_file: Path, filename: str, prompt: str, sent_at: datetime, status: str, error: str = "", topic: str = ""):
    """
    TSV: filename \t prompt \t hora \t status \t error \t topic
    status: OK_PRINT | OK_NOPRINT | ERROR
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)
    clean_prompt = (prompt or "").replace("\t", " ").replace("\n", " ").strip()
    clean_filename = (filename or "").replace("\t", " ").strip()
    clean_error = (error or "").replace("\t", " ").replace("\n", " ").strip()
    clean_topic = (topic or "").replace("\t", " ").strip()
    line = f"{clean_filename}\t{clean_prompt}\t{sent_at.isoformat()}\t{status}\t{clean_error}\t{clean_topic}\n"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line)


def prepare_for_thermal(src_img: Image.Image, target_width: int) -> Image.Image:
    img = ImageOps.exif_transpose(src_img.convert("L"))
    if img.width != target_width:
        new_h = int(round(img.height * (target_width / img.width)))
        img = img.resize((target_width, new_h))
    return img.convert("1")


def generate_image_bytes(client: OpenAI, prompt: str) -> bytes:
    result = client.images.generate(
        model=OPENAI_MODEL,
        prompt=prompt,
        size=OPENAI_SIZE,
        output_format=OPENAI_FORMAT,
    )
    return base64.b64decode(result.data[0].b64_json)


def save_image(image_bytes: bytes, out_dir: Path, filename: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / filename
    with open(p, "wb") as f:
        f.write(image_bytes)
    return p


def print_text_and_image_escpos(printer: EscposSerial, text: str, image_path: Path, printer_width: int):
    printer.text(text)
    printer.text("\n\n")
    with Image.open(image_path) as im:
        im_ready = prepare_for_thermal(im, printer_width)
    printer.image(im_ready, impl="bitImageRaster")
    try:
        printer.cut()
    except Exception:
        printer.text("\n\n\n")


def parse_iso_dt(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# =========================
# Data model
# =========================
@dataclass
class Item:
    filename: str
    prompt: str
    ts: str
    status: str
    image_path: str
    topic: str
    group: str


# =========================
# Worker: MQTT listener + job processor
# =========================
class Worker:
    def __init__(self, ui_q: "queue.Queue[dict]"):
        self.ui_q = ui_q
        self.stop_evt = threading.Event()

        self.proc_thread: threading.Thread | None = None
        self.mqtt_thread: threading.Thread | None = None

        self.job_q: "queue.Queue[dict]" = queue.Queue()

        self.client_openai: OpenAI | None = None
        self.printer: EscposSerial | None = None
        self.mqtt_client: mqtt.Client | None = None

        self.cfg = {}
        self._lock = threading.Lock()

    def start(self, cfg: dict):
        with self._lock:
            if self.proc_thread and self.proc_thread.is_alive():
                self.stop_evt.set()
        self._join_threads(timeout=4)

        with self._lock:
            self.cfg = cfg
            self.stop_evt.clear()

            while True:
                try:
                    self.job_q.get_nowait()
                except queue.Empty:
                    break
                except Exception:
                    break

            self.proc_thread = threading.Thread(target=self._process_loop, daemon=True)
            self.proc_thread.start()

            self.mqtt_thread = threading.Thread(target=self._mqtt_loop, daemon=True)
            self.mqtt_thread.start()

    def stop(self):
        with self._lock:
            self.stop_evt.set()

            try:
                if self.mqtt_client:
                    try:
                        self.mqtt_client.loop_stop()
                    except Exception:
                        pass
                    try:
                        self.mqtt_client.disconnect()
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                if self.printer:
                    self.printer.close()
            except Exception:
                pass

        self._join_threads(timeout=6)

        with self._lock:
            self.mqtt_client = None
            self.printer = None
            self.client_openai = None
            self.mqtt_thread = None
            self.proc_thread = None

    def _join_threads(self, timeout: float = 3.0):
        try:
            if self.mqtt_thread and self.mqtt_thread.is_alive():
                self.mqtt_thread.join(timeout=timeout)
        except Exception:
            pass
        try:
            if self.proc_thread and self.proc_thread.is_alive():
                self.proc_thread.join(timeout=timeout)
        except Exception:
            pass

    def _mqtt_loop(self):
        host = self.cfg["mqtt_host"]
        port = int(self.cfg["mqtt_port"])
        tls_on = bool(self.cfg["mqtt_tls"])
        username = (self.cfg.get("mqtt_user") or "").strip()
        password = (self.cfg.get("mqtt_pass") or "").strip()
        topics = [t.strip() for t in self.cfg.get("mqtt_topics", []) if (t or "").strip()] or DEFAULT_TOPICS[:]

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        with self._lock:
            self.mqtt_client = client

        if username:
            client.username_pw_set(username, password or None)

        if tls_on:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)

        def on_connect(cl, userdata, flags, reason_code, properties):
            if reason_code == 0:
                self.ui_q.put({"type": "status", "msg": f"MQTT connectat ({host}:{port})"})
                for t in topics:
                    try:
                        cl.subscribe(t, qos=0)
                        self.ui_q.put({"type": "topic_subscribed", "topic": t})
                        self.ui_q.put({"type": "status", "msg": f"Subscrit a: {t}"})
                    except Exception as e:
                        self.ui_q.put({"type": "status", "msg": f"Error subscrivint ({t}): {e}"})
            else:
                self.ui_q.put({"type": "status", "msg": f"Error connectant MQTT reason_code={reason_code}"})

        def on_message(cl, userdata, msg):
            try:
                payload = msg.payload.decode("utf-8", errors="replace")
                raw = safe_line(payload)
                if not raw:
                    return

                group, prompt = split_group_and_prompt(raw)
                if not prompt:
                    return

                ts = datetime.now()
                topic = msg.topic or ""

                job_id = hashlib.sha1(
                    f"{ts.isoformat()}|{topic}|{group}|{prompt}".encode("utf-8", errors="ignore")
                ).hexdigest()[:12]

                self.ui_q.put({
                    "type": "queued",
                    "job_id": job_id,
                    "ts": ts.isoformat(timespec="seconds"),
                    "prompt": prompt,
                    "topic": topic,
                    "group": group,
                })

                self.job_q.put({
                    "job_id": job_id,
                    "ts": ts,
                    "prompt": prompt,
                    "topic": topic,
                    "group": group,
                })
            except Exception as e:
                self.ui_q.put({"type": "status", "msg": f"Error missatge MQTT: {e}"})

        client.on_connect = on_connect
        client.on_message = on_message

        self.ui_q.put({"type": "status", "msg": f"Connectant a MQTT {host}:{port} (TLS={'Sí' if tls_on else 'No'})..."})

        try:
            client.connect(host, port, keepalive=60)
        except Exception as e:
            self.ui_q.put({"type": "fatal", "msg": f"No puc connectar a MQTT {host}:{port}: {e}"})
            return

        try:
            client.loop_start()
            while not self.stop_evt.is_set():
                time.sleep(0.2)
        finally:
            try:
                client.loop_stop()
            except Exception:
                pass
            try:
                client.disconnect()
            except Exception:
                pass
            self.ui_q.put({"type": "status", "msg": "MQTT aturat"})

    def _process_loop(self):
        api_key = self.cfg["api_key"]
        printer_port = (self.cfg.get("printer_port") or "").strip()
        printer_baud = int(self.cfg["printer_baud"])
        printer_width = int(self.cfg["printer_width"])
        out_dir = Path(self.cfg["out_dir"])
        log_file = Path(self.cfg["log_file"])

        if not api_key:
            self.ui_q.put({"type": "fatal", "msg": "La clau API és buida"})
            return

        with self._lock:
            self.client_openai = OpenAI(api_key=api_key)

        # Printer OPTIONAL
        if printer_port:
            try:
                pr = EscposSerial(devfile=printer_port, baudrate=printer_baud, timeout=5)
                with self._lock:
                    self.printer = pr
                self.ui_q.put({"type": "status", "msg": f"Impressora OK ({printer_port})"})
            except Exception as e:
                with self._lock:
                    self.printer = None
                self.ui_q.put({"type": "status", "msg": f"⚠️ Impressora NO disponible ({printer_port}). Continuo sense imprimir. Error: {e}"})
        else:
            with self._lock:
                self.printer = None
            self.ui_q.put({"type": "status", "msg": "⚠️ Sense port d’impressora. Continuo sense imprimir."})

        self.ui_q.put({"type": "status", "msg": "En marxa"})

        try:
            while not self.stop_evt.is_set():
                try:
                    job = self.job_q.get(timeout=0.2)
                except queue.Empty:
                    continue

                if self.stop_evt.is_set():
                    break

                job_id = job["job_id"]
                ts: datetime = job["ts"]
                prompt: str = job["prompt"]
                topic: str = job.get("topic", "")
                group: str = job.get("group", "")

                prefix = slugify_group_prefix(group) if group else ""
                filename = build_filename(prefix=prefix, seed=f"{topic}|{group}|{prompt}", ext=OPENAI_FORMAT)

                self.ui_q.put({"type": "processing", "job_id": job_id, "msg": "Generant imatge..."})

                try:
                    img_bytes = generate_image_bytes(self.client_openai, prompt)
                    img_path = save_image(img_bytes, out_dir, filename)

                    printable = (
                        "mSchools — Prompt MQTT\n"
                        f"Topic: {topic}\n"
                        f"Grup: {group}\n"
                        f"Hora: {ts.isoformat(timespec='seconds')}\n"
                        f"Fitxer: {filename}\n\n"
                        "PROMPT:\n"
                        f"{prompt}\n"
                    )

                    did_print = False
                    if self.printer is not None:
                        try:
                            self.ui_q.put({"type": "processing", "job_id": job_id, "msg": "Imprimint..."})
                            print_text_and_image_escpos(self.printer, printable, img_path, printer_width)
                            did_print = True
                        except Exception as e:
                            self.ui_q.put({"type": "status", "msg": f"⚠️ Error imprimint: {e}. Continuo."})
                            did_print = False
                    else:
                        self.ui_q.put({"type": "status", "msg": "🟡 Impressió omesa (no hi ha impressora detectada)."})

                    append_log_tsv(log_file, filename, prompt, ts, "OK_PRINT" if did_print else "OK_NOPRINT", "", topic)

                    self.ui_q.put({
                        "type": "done",
                        "job_id": job_id,
                        "status": "OK",
                        "ts": ts.isoformat(timespec="seconds"),
                        "prompt": prompt,
                        "filename": filename,
                        "image_path": str(img_path),
                        "topic": topic,
                        "group": group,
                        "printed": did_print,
                    })

                except Exception as e:
                    append_log_tsv(log_file, filename, prompt, ts, "ERROR", str(e), topic)
                    self.ui_q.put({
                        "type": "done",
                        "job_id": job_id,
                        "status": "ERROR",
                        "ts": ts.isoformat(timespec="seconds"),
                        "prompt": prompt,
                        "filename": filename,
                        "image_path": "",
                        "error": str(e),
                        "topic": topic,
                        "group": group,
                        "printed": False,
                    })
        finally:
            try:
                with self._lock:
                    if self.printer:
                        self.printer.close()
            except Exception:
                pass
            self.ui_q.put({"type": "status", "msg": "Aturat"})


# =========================
# UI
# =========================
class App(tb.Window):
    def __init__(self):
        super().__init__(themename="flatly")

        self.title("mSchools — MQTT → OpenAI Image → Impressió tèrmica (ESC/POS)")
        self.state("zoomed")

        self.configure(bg=BG_WHITE)
        style = tb.Style()
        style.configure("TFrame", background=BG_WHITE)
        style.configure("TLabel", background=BG_WHITE, font=("Segoe UI", 10))
        style.configure("Heading.TLabel", font=("Segoe UI", 14, "bold"), foreground=PURPLE, background=BG_WHITE)
        style.configure("Status.TLabel", font=("Segoe UI", 10, "bold"), foreground=PURPLE, background=BG_WHITE)
        style.configure("Small.TLabel", font=("Segoe UI", 9), background=BG_WHITE)

        self.ui_q = queue.Queue()
        self.worker = Worker(self.ui_q)

        self.items: list[Item] = []
        self.thumb_refs: list[ImageTk.PhotoImage] = []
        self.selected_item_idx: int | None = None
        self.job_id_to_item_index = {}

        self._logo_ref = None
        self._detail_img_ref = None

        self.topic_status_vars: list[tk.StringVar] = []
        self.filter_var = tk.StringVar(value="Tots")
        self._known_topics: set[str] = set(DEFAULT_TOPICS)

        self._build_ui()
        self._refresh_ports()

        self._load_gallery_from_disk()
        self._poll_ui_queue()

        self.gallery_canvas.bind("<Configure>", lambda e: self._gallery_refresh_all())

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self):
        top = tb.Frame(self, padding=10)
        top.pack(side=TOP, fill=X)

        logo_box = tb.Frame(top)
        logo_box.pack(side=LEFT, anchor="nw")

        self.logo_label = tb.Label(logo_box)
        self.logo_label.pack(anchor="w")
        self._load_logo()

        cfg = tb.Frame(top)
        cfg.pack(side=RIGHT, fill=X, expand=True)

        tb.Label(cfg, text="Clau API d’OpenAI:", style="Small.TLabel").grid(row=0, column=0, sticky="w")
        self.api_key_var = tk.StringVar(value=os.environ.get("OPENAI_API_KEY", ""))
        tb.Entry(cfg, textvariable=self.api_key_var, show="*", width=52).grid(row=0, column=1, sticky="w", padx=6)

        tb.Label(cfg, text="COM impressora tèrmica:", style="Small.TLabel").grid(row=1, column=0, sticky="w")
        self.printer_var = tk.StringVar()
        self.printer_combo = tb.Combobox(cfg, textvariable=self.printer_var, width=12, state="readonly")
        self.printer_combo.grid(row=1, column=1, sticky="w", padx=6)

        tb.Label(cfg, text="Baudios:", style="Small.TLabel").grid(row=1, column=2, sticky="w")
        self.printer_baud_var = tk.IntVar(value=DEFAULT_PRINTER_BAUD)
        tb.Entry(cfg, textvariable=self.printer_baud_var, width=8).grid(row=1, column=3, sticky="w", padx=6)

        tb.Label(cfg, text="Amplada px:", style="Small.TLabel").grid(row=1, column=4, sticky="w")
        self.printer_width_var = tk.IntVar(value=DEFAULT_PRINTER_WIDTH)
        tb.Entry(cfg, textvariable=self.printer_width_var, width=8).grid(row=1, column=5, sticky="w", padx=6)

        tb.Label(cfg, text="Carpeta de sortida:", style="Small.TLabel").grid(row=2, column=0, sticky="w")
        self.out_dir_var = tk.StringVar(value=str(DEFAULT_OUT_DIR))
        tb.Entry(cfg, textvariable=self.out_dir_var, width=70).grid(row=2, column=1, columnspan=5, sticky="w", padx=6)

        tb.Label(cfg, text="Log TSV:", style="Small.TLabel").grid(row=3, column=0, sticky="w")
        self.log_file_var = tk.StringVar(value=str(DEFAULT_LOG_FILE))
        tb.Entry(cfg, textvariable=self.log_file_var, width=70).grid(row=3, column=1, columnspan=5, sticky="w", padx=6)

        tb.Label(cfg, text="Host MQTT:", style="Small.TLabel").grid(row=0, column=6, sticky="w", padx=(18, 0))
        self.mqtt_host_var = tk.StringVar(value=DEFAULT_MQTT_HOST)
        tb.Entry(cfg, textvariable=self.mqtt_host_var, width=22).grid(row=0, column=7, sticky="w", padx=6)

        tb.Label(cfg, text="Port:", style="Small.TLabel").grid(row=0, column=8, sticky="w")
        self.mqtt_port_var = tk.IntVar(value=DEFAULT_MQTT_PORT)
        tb.Entry(cfg, textvariable=self.mqtt_port_var, width=7).grid(row=0, column=9, sticky="w", padx=6)

        self.mqtt_tls_var = tk.BooleanVar(value=DEFAULT_MQTT_TLS)
        tb.Checkbutton(cfg, text="TLS", variable=self.mqtt_tls_var, bootstyle="round-toggle").grid(row=0, column=10, sticky="w")

        tb.Label(cfg, text="Usuari:", style="Small.TLabel").grid(row=1, column=6, sticky="w", padx=(18, 0))
        self.mqtt_user_var = tk.StringVar(value="")
        tb.Entry(cfg, textvariable=self.mqtt_user_var, width=16).grid(row=1, column=7, sticky="w", padx=6)

        tb.Label(cfg, text="Contrasenya:", style="Small.TLabel").grid(row=1, column=8, sticky="w")
        self.mqtt_pass_var = tk.StringVar(value="")
        tb.Entry(cfg, textvariable=self.mqtt_pass_var, show="*", width=16).grid(row=1, column=9, sticky="w", padx=6)

        tb.Label(cfg, text="Topics (4 + 1 opcional):", style="Small.TLabel").grid(row=2, column=6, sticky="w", padx=(18, 0))
        topics_box = tb.Frame(cfg)
        topics_box.grid(row=2, column=7, columnspan=4, sticky="w")

        self.topic_vars = [tk.StringVar(value=DEFAULT_TOPICS[i]) for i in range(4)]
        self.topic_status_vars = []

        for i, v in enumerate(self.topic_vars):
            tb.Entry(topics_box, textvariable=v, width=22).grid(row=0, column=i * 2, padx=(0 if i == 0 else 6, 0))
            sv = tk.StringVar(value="⬜")
            self.topic_status_vars.append(sv)
            tb.Label(topics_box, textvariable=sv, style="Small.TLabel").grid(row=0, column=i * 2 + 1, padx=(4, 0))

        fifth_row = tb.Frame(cfg)
        fifth_row.grid(row=3, column=6, columnspan=5, sticky="w", padx=(18, 0), pady=(6, 0))

        tb.Label(fifth_row, text="Topic extra:", style="Small.TLabel").pack(side=LEFT)
        self.topic5_var = tk.StringVar(value="")
        tb.Entry(fifth_row, textvariable=self.topic5_var, width=28).pack(side=LEFT, padx=(8, 0))
        self.topic5_status = tk.StringVar(value="⬜")
        tb.Label(fifth_row, textvariable=self.topic5_status, style="Small.TLabel").pack(side=LEFT, padx=(6, 0))

        btns = tb.Frame(cfg)
        btns.grid(row=0, column=11, rowspan=4, sticky="nsew", padx=12)

        tb.Button(btns, text="🔄 Refresca ports", command=self._refresh_ports, bootstyle="secondary").pack(fill=X, pady=(0, 2))
        self.test_btn = tb.Button(btns, text="🧪 Test impressora", command=self._test_printer, bootstyle="warning")
        self.test_btn.pack(fill=X, pady=(0, 2))
        self.start_btn = tb.Button(btns, text="▶ Inicia", command=self._start, bootstyle="success")
        self.start_btn.pack(fill=X, pady=(0, 2))
        self.stop_btn = tb.Button(btns, text="⏹ Atura", command=self._stop, bootstyle="danger", state="disabled")
        self.stop_btn.pack(fill=X, pady=(0, 2))

        self.print_all_btn = tb.Button(btns, text="🖨️ Imprimeix Fotos", command=self._print_all_filtered, bootstyle="primary")
        self.print_all_btn.pack(fill=X)

        self.status_var = tk.StringVar(value="Aturat")
        tb.Label(self, textvariable=self.status_var, padding=(10, 0), style="Status.TLabel").pack(anchor="w")

        main = tb.Frame(self, padding=8)
        main.pack(fill=BOTH, expand=True)

        left = tb.Frame(main)
        left.pack(side=LEFT, fill=Y)

        filter_row = tb.Frame(left)
        filter_row.pack(fill=X, pady=(0, 6))

        tb.Label(filter_row, text="Filtre de topic:", style="Small.TLabel").pack(side=LEFT)
        self.filter_combo = tb.Combobox(
            filter_row,
            textvariable=self.filter_var,
            values=["Tots"] + sorted(self._known_topics),
            state="readonly",
            width=22
        )
        self.filter_combo.pack(side=LEFT, padx=(8, 0))
        self.filter_combo.bind("<<ComboboxSelected>>", lambda _e: self._gallery_refresh_all())

        tb.Label(left, text="Galeria (clic per veure)", style="Heading.TLabel").pack(anchor="w", pady=(0, 6))

        self.gallery_canvas = tk.Canvas(left, width=380, highlightthickness=0, bg=BG_WHITE)
        self.gallery_scroll = tb.Scrollbar(left, orient="vertical", command=self.gallery_canvas.yview, bootstyle="secondary-round")
        self.gallery_frame = tb.Frame(self.gallery_canvas)

        self.gallery_frame.bind(
            "<Configure>",
            lambda e: self.gallery_canvas.configure(scrollregion=self.gallery_canvas.bbox("all"))
        )
        self.gallery_canvas.create_window((0, 0), window=self.gallery_frame, anchor="nw")
        self.gallery_canvas.configure(yscrollcommand=self.gallery_scroll.set)

        self.gallery_canvas.pack(side=LEFT, fill=Y, expand=False)
        self.gallery_scroll.pack(side=LEFT, fill=Y)

        right = tb.Frame(main, padding=(10, 0))
        right.pack(side=LEFT, fill=BOTH, expand=True)

        tb.Label(right, text="Darrer / Seleccionat", style="Heading.TLabel").pack(anchor="w")

        self.detail_prompt = tk.Text(right, height=7, wrap="word", font=("Segoe UI", 10))
        self.detail_prompt.pack(fill=X)

        self.detail_image_label = tb.Label(right)
        self.detail_image_label.pack(fill=BOTH, expand=True, pady=(8, 0))

    def _load_logo(self):
        self._logo_ref = None
        if LOGO_PATH.exists():
            try:
                im = Image.open(LOGO_PATH)
                im = ImageOps.exif_transpose(im)
                im.thumbnail((220, 80))
                self._logo_ref = ImageTk.PhotoImage(im)
                self.logo_label.configure(image=self._logo_ref, text="")
            except Exception:
                self.logo_label.configure(text="(No es pot carregar logo_FLU.png)")
        else:
            self.logo_label.configure(text="(Falta logo_FLU.png a Desktop/mSchools_Print/)")

    def _refresh_ports(self):
        ports = get_com_ports()
        self.printer_combo["values"] = ports
        if ports and not self.printer_var.get():
            self.printer_var.set(ports[0])

    def _set_running_ui(self, running: bool):
        if running:
            self.start_btn.config(state="disabled")
            self.stop_btn.config(state="normal")
            self.test_btn.config(state="disabled")
            self.print_all_btn.config(state="normal")
        else:
            self.start_btn.config(state="normal")
            self.stop_btn.config(state="disabled")
            self.test_btn.config(state="normal")
            self.print_all_btn.config(state="normal")

    def _reset_topic_checks(self):
        for sv in self.topic_status_vars:
            sv.set("⬜")
        self.topic5_status.set("⬜")

    def _update_filter_options(self):
        values = ["Tots"] + sorted(self._known_topics)
        self.filter_combo["values"] = values
        if self.filter_var.get() not in values:
            self.filter_var.set("Tots")

    def _collect_topics(self) -> list[str]:
        topics = [v.get().strip() for v in self.topic_vars if v.get().strip()]
        extra = self.topic5_var.get().strip()
        if extra:
            topics.append(extra)
        seen = set()
        out = []
        for t in topics:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def _start(self):
        api_key = self.api_key_var.get().strip()
        if not api_key:
            messagebox.showerror("Error", "Introdueix la clau API d’OpenAI.")
            return

        if not self.printer_var.get().strip():
            messagebox.showwarning(
                "Avís",
                "No s'ha detectat/seleccionat el port COM de la impressora.\n"
                "L'app seguirà funcionant i generarà imatges, però NO imprimirà fins que seleccionis un COM vàlid."
            )

        topics = self._collect_topics()
        if not topics:
            messagebox.showerror("Error", "Indica almenys 1 topic MQTT.")
            return

        self._reset_topic_checks()
        for t in topics:
            self._known_topics.add(t)
        self._update_filter_options()

        cfg = {
            "api_key": api_key,
            "printer_port": self.printer_var.get().strip(),
            "printer_baud": int(self.printer_baud_var.get()),
            "printer_width": int(self.printer_width_var.get()),
            "out_dir": self.out_dir_var.get().strip(),
            "log_file": self.log_file_var.get().strip(),
            "mqtt_host": self.mqtt_host_var.get().strip() or DEFAULT_MQTT_HOST,
            "mqtt_port": int(self.mqtt_port_var.get()),
            "mqtt_tls": bool(self.mqtt_tls_var.get()),
            "mqtt_user": self.mqtt_user_var.get().strip(),
            "mqtt_pass": self.mqtt_pass_var.get(),
            "mqtt_topics": topics,
        }

        Path(cfg["out_dir"]).mkdir(parents=True, exist_ok=True)
        Path(cfg["log_file"]).parent.mkdir(parents=True, exist_ok=True)

        self.worker.start(cfg)
        self.status_var.set("Iniciant…")
        self._set_running_ui(True)

    def _stop(self):
        self.status_var.set("Aturant…")
        try:
            self.worker.stop()
        finally:
            self.status_var.set("Aturat")
            self._set_running_ui(False)

    def _test_printer(self):
        printer_port = self.printer_var.get().strip()
        if not printer_port:
            messagebox.showerror("Error", "Selecciona el COM de la impressora.")
            return
        if not TEST_IMAGE_PATH.exists():
            messagebox.showerror("Error", f"No trobo la imatge de test:\n{TEST_IMAGE_PATH}")
            return

        try:
            printer_baud = int(self.printer_baud_var.get())
            printer_width = int(self.printer_width_var.get())

            printer = EscposSerial(devfile=printer_port, baudrate=printer_baud, timeout=5)
            try:
                print_text_and_image_escpos(
                    printer=printer,
                    text="IMPRESSORA FLU PREPARADA",
                    image_path=TEST_IMAGE_PATH,
                    printer_width=printer_width
                )
            finally:
                try:
                    printer.close()
                except Exception:
                    pass

            messagebox.showinfo("OK", "Test imprès correctament.")
        except Exception as e:
            messagebox.showerror("Error", f"No s'ha pogut imprimir el test:\n{e}")

    def _get_active_printer(self) -> EscposSerial | None:
        """
        Opens printer on demand for batch printing to avoid relying on worker internals.
        """
        printer_port = self.printer_var.get().strip()
        if not printer_port:
            return None
        try:
            return EscposSerial(devfile=printer_port, baudrate=int(self.printer_baud_var.get()), timeout=5)
        except Exception:
            return None

    def _print_all_filtered(self):
        # Determine which items are visible under current filter
        indices = self._filtered_indices()
        if not indices:
            messagebox.showinfo("Info", "No hi ha elements per imprimir amb el filtre actual.")
            return

        pr = self._get_active_printer()
        if pr is None:
            messagebox.showwarning(
                "Avís",
                "No hi ha impressora disponible (COM buit o no es pot obrir).\n"
                "No es pot fer la impressió massiva."
            )
            return

        try:
            w = int(self.printer_width_var.get())
            count = 0
            for idx in indices:
                it = self.items[idx]
                if not it.image_path or not Path(it.image_path).exists():
                    continue

                printable = (
                    "mSchools — Històric (filtre)\n"
                    f"Topic: {it.topic}\n"
                    f"Grup: {it.group}\n"
                    f"Hora: {it.ts}\n"
                    f"Fitxer: {it.filename}\n\n"
                    "PROMPT:\n"
                    f"{it.prompt}\n"
                )
                try:
                    print_text_and_image_escpos(pr, printable, Path(it.image_path), w)
                    count += 1
                except Exception:
                    # continue printing others
                    continue

            messagebox.showinfo("OK", f"Impressió massiva completada.\nElements impresos: {count}")
        finally:
            try:
                pr.close()
            except Exception:
                pass

    # ---------- Load gallery on startup ----------
    def _load_gallery_from_disk(self):
        log_path = Path(self.log_file_var.get().strip() or str(DEFAULT_LOG_FILE))
        out_dir = Path(self.out_dir_var.get().strip() or str(DEFAULT_OUT_DIR))

        if not log_path.exists():
            self._update_filter_options()
            return

        items: list[Item] = []
        topics_seen: set[str] = set()

        try:
            with open(log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) < 4:
                        continue

                    filename = parts[0]
                    prompt = parts[1] if len(parts) > 1 else ""
                    hora = parts[2] if len(parts) > 2 else ""
                    status = parts[3] if len(parts) > 3 else ""
                    topic = parts[5] if len(parts) > 5 else ""

                    if topic:
                        topics_seen.add(topic)

                    img_path = out_dir / filename
                    if status.upper().startswith("OK") and filename and img_path.exists():
                        items.append(Item(
                            filename=filename,
                            prompt=prompt,
                            ts=hora,
                            status=status,
                            image_path=str(img_path),
                            topic=topic,
                            group="",
                        ))
        except Exception:
            self._update_filter_options()
            return

        def sort_key(it: Item):
            dt = parse_iso_dt(it.ts)
            return dt or datetime.min

        items.sort(key=sort_key, reverse=True)

        self.items = items
        self._known_topics |= topics_seen
        self._update_filter_options()

        self.job_id_to_item_index = {}
        self._gallery_refresh_all()
        filt = self._filtered_indices()
        if filt:
            self._show_item(filt[0])

    # ---------- UI queue polling ----------
    def _poll_ui_queue(self):
        try:
            while True:
                ev = self.ui_q.get_nowait()
                self._handle_event(ev)
        except queue.Empty:
            pass
        self.after(120, self._poll_ui_queue)

    def _handle_event(self, ev: dict):
        typ = ev.get("type")

        if typ == "status":
            msg = ev.get("msg", "")
            self.status_var.set(msg)

        elif typ == "fatal":
            self.status_var.set("Aturat")
            messagebox.showerror("Error crític", ev.get("msg", "Error crític"))
            self._set_running_ui(False)

        elif typ == "topic_subscribed":
            t = (ev.get("topic") or "").strip()
            for i, v in enumerate(self.topic_vars):
                if v.get().strip() == t:
                    self.topic_status_vars[i].set("✅")
            if self.topic5_var.get().strip() == t and t:
                self.topic5_status.set("✅")
            if t:
                self._known_topics.add(t)
                self._update_filter_options()

        elif typ == "queued":
            job_id = ev["job_id"]
            ts = ev["ts"]
            prompt = ev["prompt"]
            topic = ev.get("topic", "")
            group = ev.get("group", "")

            if topic:
                self._known_topics.add(topic)
                self._update_filter_options()

            item = Item(filename="", prompt=prompt, ts=ts, status="PENDENT", image_path="", topic=topic, group=group)
            self.items.insert(0, item)
            self.job_id_to_item_index[job_id] = 0

            for k in list(self.job_id_to_item_index.keys()):
                if k != job_id:
                    self.job_id_to_item_index[k] += 1

            self._gallery_refresh_all()
            filt = self._filtered_indices()
            if filt:
                self._show_item(filt[0])

        elif typ == "done":
            job_id = ev["job_id"]
            status = ev.get("status", "ERROR")
            filename = ev.get("filename", "")
            image_path = ev.get("image_path", "")
            prompt = ev.get("prompt", "")
            ts = ev.get("ts", "")
            topic = ev.get("topic", "")
            group = ev.get("group", "")

            idx = self.job_id_to_item_index.get(job_id, None)
            if idx is None or idx >= len(self.items):
                return

            it = self.items[idx]
            it.status = "OK" if status == "OK" else "ERROR"
            it.filename = filename
            it.image_path = image_path
            it.prompt = prompt
            it.ts = ts
            it.topic = topic
            it.group = group

            self._gallery_refresh_all()
            if self.selected_item_idx is not None and 0 <= self.selected_item_idx < len(self.items):
                self._show_item(self.selected_item_idx)
            else:
                filt = self._filtered_indices()
                if filt:
                    self._show_item(filt[0])

            if status == "ERROR":
                err = ev.get("error", "Error")
                messagebox.showerror("Error", f"Error processant el prompt:\n{err}")

    # ---------- Filtering ----------
    def _filtered_indices(self) -> list[int]:
        sel = (self.filter_var.get() or "Tots").strip()
        if sel == "Tots":
            return list(range(len(self.items)))
        return [i for i, it in enumerate(self.items) if (it.topic or "").strip() == sel]

    # ---------- Gallery ----------
    def _gallery_clear(self):
        for child in self.gallery_frame.winfo_children():
            child.destroy()
        self.thumb_refs.clear()

    def _gallery_target_width(self) -> int:
        try:
            w = int(self.gallery_canvas.winfo_width())
        except Exception:
            w = 380
        return max(260, w - 8)

    def _gallery_refresh_all(self):
        self._gallery_clear()
        for item_idx in self._filtered_indices():
            self._gallery_add_item(item_idx)

    def _gallery_add_item(self, item_idx: int):
        item = self.items[item_idx]

        card = tb.Frame(self.gallery_frame, padding=0)
        card.pack(fill=X, pady=(0, 12))

        thumb_label = tb.Label(card)
        thumb_label.pack(anchor="w", fill=X)

        thumb = self._make_thumb_full_width(item.image_path)
        if thumb:
            thumb_label.configure(image=thumb)
            self.thumb_refs.append(thumb)
        else:
            thumb_label.configure(text="(sense imatge encara)", foreground=PURPLE)

        meta_text = f"{item.ts} — {item.status}"
        if item.topic:
            meta_text += f" — {item.topic}"
        if item.group:
            meta_text += f" — {item.group}"
        tb.Label(card, text=meta_text, style="Small.TLabel", foreground=PURPLE).pack(anchor="w", padx=2, pady=(4, 0))

        prompt_short = (item.prompt or "").strip().replace("\n", " ")
        if len(prompt_short) > 120:
            prompt_short = prompt_short[:120] + "…"
        tb.Label(card, text=prompt_short, wraplength=self._gallery_target_width(), justify="left").pack(anchor="w", padx=2)

        def on_click(_e=None, idx=item_idx):
            self._show_item(idx)

        card.bind("<Button-1>", on_click)
        for w in card.winfo_children():
            w.bind("<Button-1>", on_click)

    def _make_thumb_full_width(self, image_path: str):
        if not image_path:
            return None
        p = Path(image_path)
        if not p.exists():
            return None
        try:
            im = Image.open(p)
            im = ImageOps.exif_transpose(im)

            target_w = self._gallery_target_width()
            if im.width > 0 and target_w > 0:
                ratio = target_w / im.width
                new_h = max(1, int(im.height * ratio))
                im = im.resize((target_w, new_h))

            if im.height > 320:
                im.thumbnail((target_w, 320))

            return ImageTk.PhotoImage(im)
        except Exception:
            return None

    # ---------- Detail ----------
    def _show_item(self, item_idx: int):
        if item_idx < 0 or item_idx >= len(self.items):
            return
        self.selected_item_idx = item_idx
        item = self.items[item_idx]

        self.detail_prompt.delete("1.0", tk.END)
        header = f"{item.ts} — {item.status}\n{item.filename}\nTopic: {item.topic}\nGrup: {item.group}\n\n"
        self.detail_prompt.insert(tk.END, header + (item.prompt or ""))

        self._detail_img_ref = None
        self.detail_image_label.configure(image="", text="")

        if item.image_path and Path(item.image_path).exists():
            try:
                im = Image.open(item.image_path)
                im = ImageOps.exif_transpose(im)

                max_w = max(500, self.winfo_width() - 440)
                max_h = max(320, self.winfo_height() - 260)
                im.thumbnail((max_w, max_h))

                self._detail_img_ref = ImageTk.PhotoImage(im)
                self.detail_image_label.configure(image=self._detail_img_ref)
            except Exception:
                self.detail_image_label.configure(text="(No s'ha pogut mostrar la imatge)", foreground=PURPLE)
        else:
            self.detail_image_label.configure(text="(Encara no hi ha imatge)", foreground=PURPLE)

    def on_close(self):
        try:
            self.worker.stop()
        except Exception:
            pass
        self.destroy()


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()