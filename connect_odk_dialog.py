from PyQt5.QtWidgets import QDialog, QComboBox, QLineEdit, QPushButton, QVBoxLayout, QFormLayout, QMessageBox,QApplication
from qgis.core import QgsVectorLayer, QgsProject
from qgis.gui import QgsMapCanvas  # Ensure QgsMapCanvas is imported from qgis.gui
from PyQt5.QtWidgets import QVBoxLayout, QPushButton, QHBoxLayout
from qgis.core import QgsVectorLayer, QgsProject, QgsCoordinateReferenceSystem
import requests

from PyQt5.QtWidgets import (QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, 
                             QLineEdit, QFileDialog, QComboBox, QHBoxLayout, QMessageBox, 
                             QTextEdit, QSpinBox)
import json 
from PyQt5.QtCore import Qt  # Add this import for Qt
from qgis.core import QgsVectorLayer, QgsProject
from PyQt5.QtCore import QTimer

#------- Showing dialog
from PyQt5.QtWidgets import QDialog, QProgressBar, QVBoxLayout, QMessageBox, QPushButton
from PyQt5.QtCore import Qt

import csv
import json
from qgis.PyQt.QtWidgets import QFileDialog, QMessageBox
from PyQt5.QtCore import QSettings  

 
from qgis.core import QgsMessageLog
from collections import OrderedDict

from PyQt5.QtWidgets import QLabel
from PyQt5.QtGui import QPixmap


from PyQt5.QtWidgets import QDialog, QComboBox, QLineEdit, QPushButton, QVBoxLayout, QFormLayout, QMessageBox, QTextEdit, QProgressBar, QLabel, QWidget
from qgis.core import QgsVectorLayer, QgsProject, QgsCoordinateReferenceSystem
from qgis.gui import QgsMapCanvas
from PyQt5.QtCore import Qt, QTimer, QSettings
from PyQt5.QtGui import QPixmap
import requests
import json
import csv
from qgis.PyQt.QtWidgets import QFileDialog
from collections import OrderedDict

from datetime import datetime  # Ensure this is in imports
from PyQt5.QtCore import QThread, pyqtSignal, QObject

import tempfile
import os
import re
import shutil
import time
from pathlib import Path

from .help_panel import CollapsibleHelpMixin, configure_qgis_dialog

# ODK Central HTTP timeouts: (connect seconds, read seconds)
ODK_CONNECT_TIMEOUT = 30
ODK_READ_TIMEOUT = 1800  # 30 minutes
ODK_PAGE_SIZE = 100
ODK_MIN_PAGE_SIZE = 10
ODK_MAX_RETRIES = 3
ODK_RETRY_DELAYS = (2, 5, 10)
ODK_RETRYABLE_HTTP_CODES = {500, 502, 503, 504}
ODK_RESUME_FALLBACK_PAGE_SIZE = 250
ODK_INDIVIDUAL_THRESHOLD = 250
ODK_INDIVIDUAL_BATCH_SIZE = 25
# Past this record offset, use small bulk $expand pages (tested: top=5 works, top=250 504s).
ODK_HIGH_SKIP_THRESHOLD = 1500
ODK_HIGH_SKIP_BATCH_SIZE = 5

# Optional imports for advanced functionality
try:
    import geopandas as gpd
    import pandas as pd
    import shortuuid
    from shapely.geometry import mapping, shape
    from fuzzywuzzy import fuzz
    from rapidfuzz import process, fuzz
except ImportError:
    # These are optional and only needed for certain features
    pass

try:
    from shapely import force_2d
except ImportError:
    def force_2d(geom):
        """Fallback to convert geometry to 2D by dropping Z coordinate."""
        if geom is None:
            return None
        geom_dict = mapping(geom)
        if geom_dict["type"] == "Point":
            geom_dict["coordinates"] = geom_dict["coordinates"][:2]
        elif geom_dict["type"] in ["LineString", "LinearRing"]:
            geom_dict["coordinates"] = [coord[:2] for coord in geom_dict["coordinates"]]
        elif geom_dict["type"] == "Polygon":
            geom_dict["coordinates"] = [[coord[:2] for coord in ring] for ring in geom_dict["coordinates"]]
        elif geom_dict["type"] in ["MultiPoint", "MultiLineString", "MultiPolygon"]:
            geom_dict["coordinates"] = [
                force_2d(shape(sub_geom)).__geo_interface__["coordinates"]
                for sub_geom in geom_dict["coordinates"]
            ]
        return shape(geom_dict)


class DownloadCheckpoint:
    """Persist submission pages to disk so downloads can resume after failure."""

    META_FILE = "meta.json"
    PAGES_DIR = "pages"
    PARTIAL_FILE = "submissions_partial.json"

    def __init__(self, checkpoint_dir):
        self.dir = Path(checkpoint_dir)
        self.pages_dir = self.dir / self.PAGES_DIR
        self.meta_path = self.dir / self.META_FILE
        self.partial_path = self.dir / self.PARTIAL_FILE

    @staticmethod
    def form_key(project_id, form_id):
        safe_form = re.sub(r"[^\w.-]", "_", str(form_id))
        return f"project_{project_id}_{safe_form}"

    @classmethod
    def get_checkpoint_dir(cls, base_dir, project_id, form_id):
        return Path(base_dir) / cls.form_key(project_id, form_id)

    def read_meta(self):
        if not self.meta_path.exists():
            return None
        with open(self.meta_path, encoding="utf-8") as handle:
            return json.load(handle)

    def matches(self, server_url, project_id, form_id):
        meta = self.read_meta()
        if not meta:
            return False
        return (
            meta.get("server_url") == server_url.rstrip("/")
            and meta.get("project_id") == project_id
            and meta.get("form_id") == form_id
        )

    def init_download(self, server_url, project_id, form_id, total_count, page_size):
        self.dir.mkdir(parents=True, exist_ok=True)
        self.pages_dir.mkdir(parents=True, exist_ok=True)
        self._write_meta(
            {
                "server_url": server_url.rstrip("/"),
                "project_id": project_id,
                "form_id": form_id,
                "total_count": total_count,
                "page_size": page_size,
                "effective_page_size": page_size,
                "individual_mode": False,
                "next_skip": 0,
                "record_count": 0,
                "complete": False,
                "updated_at": datetime.now().isoformat(),
            }
        )

    def load_resume_state(self):
        meta = self.read_meta()
        if not meta or meta.get("complete"):
            return 0, [], None, False
        records = self.load_records()
        next_skip = int(meta.get("next_skip", len(records)))
        effective_page_size = meta.get("effective_page_size")
        individual_mode = bool(meta.get("individual_mode"))
        return next_skip, records, effective_page_size, individual_mode

    def set_individual_mode(self, enabled=True):
        meta = self.read_meta()
        if not meta:
            return
        meta["individual_mode"] = bool(enabled)
        if enabled:
            meta["effective_page_size"] = 1
        meta["updated_at"] = datetime.now().isoformat()
        self._write_meta(meta)

    def update_effective_page_size(self, size):
        meta = self.read_meta()
        if not meta:
            return
        size = max(ODK_MIN_PAGE_SIZE, int(size))
        current = int(meta.get("effective_page_size", size))
        if size >= current:
            return
        meta["effective_page_size"] = size
        meta["updated_at"] = datetime.now().isoformat()
        self._write_meta(meta)

    def load_records(self):
        if not self.pages_dir.exists():
            return []
        records = []
        for page_file in sorted(self.pages_dir.glob("skip_*.json")):
            with open(page_file, encoding="utf-8") as handle:
                batch = json.load(handle)
            if isinstance(batch, list):
                records.extend(batch)
        return records

    def page_count(self):
        if not self.pages_dir.exists():
            return 0
        return len(list(self.pages_dir.glob("skip_*.json")))

    def save_page(self, skip, records):
        if not records:
            return 0
        self.dir.mkdir(parents=True, exist_ok=True)
        self.pages_dir.mkdir(parents=True, exist_ok=True)
        page_path = self.pages_dir / f"skip_{int(skip):09d}.json"
        with open(page_path, "w", encoding="utf-8") as handle:
            json.dump(records, handle)

        meta = self.read_meta() or {}
        next_skip = int(skip) + len(records)
        meta["next_skip"] = next_skip
        meta["record_count"] = next_skip
        meta["complete"] = False
        meta["updated_at"] = datetime.now().isoformat()
        self._write_meta(meta)

        with open(self.partial_path, "w", encoding="utf-8") as handle:
            json.dump(self.load_records(), handle)
        return next_skip

    def _write_meta(self, meta):
        with open(self.meta_path, "w", encoding="utf-8") as handle:
            json.dump(meta, handle, indent=2)

    def clear(self):
        if self.dir.exists():
            shutil.rmtree(self.dir)


class SubmissionWorker(QObject):
    """Worker to fetch submissions in a background thread."""
    progress = pyqtSignal(int)  # Emit progress percentage
    status = pyqtSignal(str)  # Emit progress bar status text
    log = pyqtSignal(str)  # Emit log messages
    finished = pyqtSignal()  # Signal when done
    result = pyqtSignal(list, bool)  # submissions, download_complete
    error = pyqtSignal(str)  # Emit error message

    def __init__(
        self,
        server_url,
        username,
        password,
        project_id,
        form_id,
        page_size=ODK_PAGE_SIZE,
        checkpoint_dir=None,
        read_timeout=ODK_READ_TIMEOUT,
        download_all=True,
    ):
        super().__init__()
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.password = password
        self.project_id = project_id
        self.form_id = form_id
        self.page_size = max(1, int(page_size))
        self.read_timeout = max(60, int(read_timeout))
        self.download_all = bool(download_all)
        self.checkpoint = (
            DownloadCheckpoint(checkpoint_dir) if checkpoint_dir else None
        )
        self._individual_mode = False
        self._logged_high_skip = False
        self._is_running = True

    def stop(self):
        """Signal the worker to stop execution."""
        self._is_running = False

    @staticmethod
    def _format_duration(seconds):
        seconds = max(int(seconds), 0)
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m"
        if minutes:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    def _request(self, url, headers):
        return requests.get(
            url,
            auth=(self.username, self.password),
            headers=headers,
            timeout=(ODK_CONNECT_TIMEOUT, self.read_timeout),
        )

    @staticmethod
    def _is_retryable_error(exc):
        if isinstance(
            exc,
            (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ),
        ):
            return True
        if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
            return exc.response.status_code in ODK_RETRYABLE_HTTP_CODES
        return False

    def _remember_page_size(self, size):
        if self.checkpoint:
            self.checkpoint.update_effective_page_size(size)

    def _enable_individual_mode(self, reason="Bulk download failing"):
        if self._individual_mode:
            return
        self._individual_mode = True
        self._remember_page_size(1)
        if self.checkpoint:
            self.checkpoint.set_individual_mode(True)
        self.log.emit(
            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
            f"{reason} — switching to one-by-one mode as last resort."
        )

    def _resolve_request_size(self, skip, current_page_size, remaining_count=None):
        size = current_page_size
        if remaining_count is not None:
            size = min(size, remaining_count) if remaining_count else size
        if not self._individual_mode and not self.download_all and skip >= ODK_HIGH_SKIP_THRESHOLD:
            size = min(size, ODK_HIGH_SKIP_BATCH_SIZE)
        return max(1, size)

    def _request_json_with_retries(self, url, headers, context):
        last_error = None
        for attempt in range(ODK_MAX_RETRIES):
            if not self._is_running:
                return {}
            try:
                response = self._request(url, headers)
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                last_error = exc
                if not self._is_retryable_error(exc) or attempt >= ODK_MAX_RETRIES - 1:
                    raise
                delay = ODK_RETRY_DELAYS[min(attempt, len(ODK_RETRY_DELAYS) - 1)]
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Retry {attempt + 2}/{ODK_MAX_RETRIES} for {context} after {delay}s ({exc})"
                )
                time.sleep(delay)
        raise last_error

    @staticmethod
    def _odata_quote(value):
        return str(value).replace("'", "''")

    def _fetch_submission_ids(self, base_url, headers, skip, top):
        page_url = f"{base_url}?%24top={top}&%24skip={skip}&%24select=__id"
        data = self._request_json_with_retries(
            page_url,
            headers,
            f"submission IDs {skip + 1}-{skip + top}",
        )
        return [
            row["__id"]
            for row in self._parse_submission_batch(data)
            if isinstance(row, dict) and row.get("__id")
        ]

    def _fetch_one_submission(self, base_url, headers, submission_id, record_skip):
        safe_id = self._odata_quote(submission_id)
        page_url = f"{base_url}?$filter=__id eq '{safe_id}'&%24expand=*"
        data = self._request_json_with_retries(
            page_url,
            headers,
            f"record {record_skip + 1}",
        )
        batch = self._parse_submission_batch(data)
        if self.checkpoint and batch:
            self.checkpoint.save_page(record_skip, batch)
        return batch

    def _fetch_individually(self, base_url, headers, skip, count, total_count=None, fetched_base=0):
        if not self._individual_mode:
            self._enable_individual_mode()

        self.log.emit(
            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
            f"Fetching records {skip + 1}-{skip + count} one at a time..."
        )
        submission_ids = self._fetch_submission_ids(base_url, headers, skip, count)
        collected = []

        for index, submission_id in enumerate(submission_ids):
            if not self._is_running:
                break
            record_skip = skip + index
            fetched_so_far = fetched_base + len(collected) + 1
            if total_count:
                percent = min(99, int((fetched_so_far / total_count) * 100))
                self.progress.emit(percent)
                self.status.emit(f"Record {fetched_so_far}/{total_count}")
            self.log.emit(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                f"Fetching record {record_skip + 1}"
                f"{f' ({fetched_so_far}/{total_count})' if total_count else ''}..."
            )
            batch = self._fetch_one_submission(
                base_url, headers, submission_id, record_skip
            )
            collected.extend(batch)

        return collected

    def _parse_submission_batch(self, data):
        if not isinstance(data, dict):
            raise Exception("Unexpected response format. Expected a dictionary.")
        batch = data.get("value", [])
        if not isinstance(batch, list):
            raise Exception("Unexpected response format. Expected a list in 'value'.")
        return batch

    def _fetch_single_batch(self, base_url, headers, skip, top):
        page_url = f"{base_url}?%24top={top}&%24skip={skip}&%24expand=*"
        last_error = None
        for attempt in range(ODK_MAX_RETRIES):
            if not self._is_running:
                return []
            try:
                response = self._request(page_url, headers)
                response.raise_for_status()
                batch = self._parse_submission_batch(response.json())
                if self.checkpoint and batch:
                    self.checkpoint.save_page(skip, batch)
                return batch
            except Exception as exc:
                last_error = exc
                if not self._is_retryable_error(exc) or attempt >= ODK_MAX_RETRIES - 1:
                    raise
                delay = ODK_RETRY_DELAYS[min(attempt, len(ODK_RETRY_DELAYS) - 1)]
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Retry {attempt + 2}/{ODK_MAX_RETRIES} for records {skip + 1}-"
                    f"{skip + top} after {delay}s ({exc})"
                )
                time.sleep(delay)
        raise last_error

    def _fetch_batch_resilient(self, base_url, headers, skip, batch_size):
        """Fetch a batch, splitting into smaller requests if the server times out.

        Returns (records, effective_chunk_size) where effective_chunk_size is the
        smallest chunk size used (may be less than batch_size after a split).
        """
        try:
            batch = self._fetch_single_batch(base_url, headers, skip, batch_size)
            return batch, batch_size
        except Exception as exc:
            if not self._is_retryable_error(exc):
                raise
            if batch_size <= ODK_MIN_PAGE_SIZE:
                collected = self._fetch_individually(
                    base_url, headers, skip, batch_size
                )
                return collected, 1

            smaller = max(ODK_MIN_PAGE_SIZE, batch_size // 2)
            self._remember_page_size(smaller)
            self.log.emit(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                f"Request failed for {batch_size} record(s) at offset {skip + 1}. "
                f"Retrying in smaller chunks of {smaller}..."
            )

            collected = []
            offset = skip
            remaining = batch_size
            while remaining > 0 and self._is_running:
                chunk_size = min(smaller, remaining)
                chunk, _ = self._fetch_batch_resilient(base_url, headers, offset, chunk_size)
                collected.extend(chunk)
                offset += len(chunk)
                remaining -= len(chunk)
                if len(chunk) < chunk_size:
                    break
            return collected, smaller

    def _fetch_submission_count(self, base_url, headers):
        count_url = f"{base_url}?$count=true&%24top=0"
        try:
            response = self._request(count_url, headers)
            response.raise_for_status()
            data = response.json()
            count = data.get("@odata.count")
            if count is not None:
                return int(count)
        except Exception as exc:
            self.log.emit(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                f"Could not read submission count from server ({exc}). "
                "Progress will update by page instead."
            )
        return None

    def _resolve_resume_page_size(self, saved_effective_size, individual_mode=False, skip=0):
        if (
            not self.download_all
            and skip >= ODK_HIGH_SKIP_THRESHOLD
            and not individual_mode
        ):
            return ODK_HIGH_SKIP_BATCH_SIZE
        if individual_mode:
            return ODK_INDIVIDUAL_BATCH_SIZE
        if saved_effective_size:
            return min(self.page_size, int(saved_effective_size))
        return min(self.page_size, ODK_RESUME_FALLBACK_PAGE_SIZE)

    def _emit_partial_failure(self, exc):
        all_submissions = self._sync_from_checkpoint()
        if not all_submissions:
            return False
        checkpoint_hint = ""
        if self.checkpoint:
            checkpoint_hint = (
                f" Saved to checkpoint ({self.checkpoint.partial_path}). "
                "Click Process Form again to resume from the failed page."
            )
        self.log.emit(
            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
            f"Stopped after {len(all_submissions)} record(s) due to: {exc}"
            f"{checkpoint_hint}"
        )
        self.error.emit(
            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
            f"Download stopped after {len(all_submissions)} record(s)."
            f"{checkpoint_hint} Error: {exc}"
        )
        self.result.emit(all_submissions, False)
        return True

    def _fetch_all_submissions(self, base_url, headers, total_count):
        top = total_count if total_count else 50000
        page_url = f"{base_url}?%24top={top}&%24skip=0&%24expand=*"
        self.status.emit(f"Downloading all {top} submission(s)...")
        self.log.emit(
            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
            f"Requesting all {top} submission(s) in one download..."
        )
        data = self._request_json_with_retries(
            page_url, headers, f"all {top} submissions"
        )
        batch = self._parse_submission_batch(data)
        if self.checkpoint and batch:
            self.checkpoint.save_page(0, batch)
        return batch

    def _sync_from_checkpoint(self):
        if self.checkpoint:
            return self.checkpoint.load_records()
        return []

    def run(self):
        """Fetch submissions in pages from ODK Central."""
        try:
            if not self._is_running:
                self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Worker stopped before starting.")
                self.result.emit([], True)
                self.finished.emit()
                return

            headers = {"Accept": "application/json"}
            base_url = (
                f"{self.server_url}/v1/projects/{self.project_id}/forms/"
                f"{self.form_id}.svc/Submissions"
            )
            self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Initiating submission fetch...")
            self.log.emit(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                f"Read timeout: {self.read_timeout // 60} min per request."
            )
            if self.download_all:
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    "Mode: download all submissions in one request."
                )
            else:
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Mode: paged download, page size {self.page_size}."
                )
            self.progress.emit(0)
            self.status.emit("Checking submission count...")

            total_count = self._fetch_submission_count(base_url, headers)

            if self.download_all:
                start_time = time.time()
                if self.checkpoint:
                    if self.checkpoint.dir.exists():
                        self.checkpoint.clear()
                    self.checkpoint.init_download(
                        self.server_url,
                        self.project_id,
                        self.form_id,
                        total_count,
                        total_count or self.page_size,
                    )
                if total_count is not None:
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"Server reports {total_count} submission(s)."
                    )
                else:
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        "Submission count unknown — requesting up to 50000 records."
                    )
                if not self._is_running:
                    self.result.emit([], True)
                    self.finished.emit()
                    return
                try:
                    all_submissions = self._fetch_all_submissions(
                        base_url, headers, total_count
                    )
                except Exception as exc:
                    if self._emit_partial_failure(exc):
                        self.finished.emit()
                        return
                    raise
                total_downloaded = len(all_submissions)
                elapsed = self._format_duration(time.time() - start_time)
                if total_downloaded == 0:
                    self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No submissions found.")
                    if self.checkpoint:
                        self.checkpoint.clear()
                else:
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"Downloaded {total_downloaded} submission(s) in {elapsed}."
                    )
                self.progress.emit(100)
                self.status.emit(f"Downloaded {total_downloaded} submission(s)")
                if self.checkpoint and total_downloaded > 0:
                    self.checkpoint.clear()
                self.result.emit(all_submissions, True)
                self.finished.emit()
                return

            self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Initiating paged submission fetch...")
            if total_count is not None:
                total_pages = max(1, (total_count + self.page_size - 1) // self.page_size)
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Server reports {total_count} submission(s) across ~{total_pages} page(s)."
                )
            else:
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    "Downloading pages until a short page is returned."
                )

            all_submissions = []
            skip = 0
            page = 0
            start_time = time.time()
            current_page_size = self.page_size

            if self.checkpoint:
                meta = self.checkpoint.read_meta()
                if (
                    self.checkpoint.matches(self.server_url, self.project_id, self.form_id)
                    and meta
                    and not meta.get("complete")
                    and int(meta.get("record_count", 0)) > 0
                ):
                    skip, all_submissions, saved_effective_size, individual_mode = (
                        self.checkpoint.load_resume_state()
                    )
                    page = self.checkpoint.page_count()
                    if individual_mode and skip >= ODK_HIGH_SKIP_THRESHOLD:
                        individual_mode = False
                        self.checkpoint.set_individual_mode(False)
                        self.log.emit(
                            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                            f"High-offset resume at record {skip + 1}: "
                            f"using small bulk batches of {ODK_HIGH_SKIP_BATCH_SIZE}."
                        )
                    elif (
                        individual_mode
                        and saved_effective_size
                        and int(saved_effective_size) > ODK_MIN_PAGE_SIZE
                    ):
                        individual_mode = False
                        self.checkpoint.set_individual_mode(False)
                    if individual_mode:
                        self._individual_mode = True
                    current_page_size = self._resolve_resume_page_size(
                        saved_effective_size, self._individual_mode, skip
                    )
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"Resuming download from record {skip + 1} "
                        f"({len(all_submissions)} already saved in checkpoint)."
                    )
                    if self._individual_mode:
                        self.log.emit(
                            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                            f"One-by-one mode active ({current_page_size} records per step)."
                        )
                    elif current_page_size < self.page_size:
                        self._remember_page_size(current_page_size)
                        self.log.emit(
                            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                            f"Using reduced batch size {current_page_size} from checkpoint."
                        )
                    if self.checkpoint.partial_path.exists():
                        self.log.emit(
                            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                            f"Checkpoint file: {self.checkpoint.partial_path}"
                        )
                else:
                    if self.checkpoint.dir.exists():
                        self.checkpoint.clear()
                    self.checkpoint.init_download(
                        self.server_url,
                        self.project_id,
                        self.form_id,
                        total_count,
                        self.page_size,
                    )
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"Checkpoint folder: {self.checkpoint.dir}"
                    )

            while self._is_running:
                page += 1
                remaining_count = (
                    max(total_count - skip, 0) if total_count is not None else None
                )
                request_size = self._resolve_request_size(
                    skip, current_page_size, remaining_count
                )
                if (
                    not self._logged_high_skip
                    and not self.download_all
                    and skip >= ODK_HIGH_SKIP_THRESHOLD
                    and not self._individual_mode
                    and request_size <= ODK_HIGH_SKIP_BATCH_SIZE
                ):
                    self._logged_high_skip = True
                    self._remember_page_size(ODK_HIGH_SKIP_BATCH_SIZE)
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"High-offset range (record {skip + 1}+): "
                        f"using small bulk batches of {ODK_HIGH_SKIP_BATCH_SIZE}."
                    )
                page_start = skip + 1
                page_end = skip + request_size
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Fetching page {page} (records {page_start}-{page_end}, "
                    f"batch size {request_size})..."
                )
                self.status.emit(f"Fetching page {page}...")

                try:
                    if self._individual_mode:
                        batch = self._fetch_individually(
                            base_url,
                            headers,
                            skip,
                            request_size,
                            total_count=total_count,
                            fetched_base=len(all_submissions),
                        )
                        effective_size = 1
                    else:
                        batch, effective_size = self._fetch_batch_resilient(
                            base_url, headers, skip, request_size
                        )
                except Exception as exc:
                    if self._emit_partial_failure(exc):
                        self.finished.emit()
                        return
                    raise

                if effective_size < current_page_size:
                    current_page_size = effective_size
                    self._remember_page_size(current_page_size)
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"Reducing page size to {current_page_size} for remaining downloads."
                    )

                all_submissions.extend(batch)
                fetched = len(all_submissions)
                elapsed = time.time() - start_time
                eta_text = ""

                if fetched > 0 and elapsed > 0:
                    if total_count and fetched < total_count:
                        remaining = total_count - fetched
                        eta_sec = int((elapsed / fetched) * remaining)
                        eta_text = f", ETA ~{self._format_duration(eta_sec)}"
                    elif len(batch) == request_size:
                        avg_page_time = elapsed / page
                        eta_text = f", ~{avg_page_time:.1f}s per page"

                if total_count:
                    percent = min(99, int((fetched / total_count) * 100))
                    status = f"Page {page}: {fetched}/{total_count}{eta_text}"
                else:
                    percent = min(95, page * 10)
                    status = f"Page {page}: {fetched} downloaded{eta_text}"

                self.progress.emit(percent)
                self.status.emit(status)
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Page {page} complete: {len(batch)} record(s), total {fetched}{eta_text}"
                )
                if self.checkpoint:
                    self.log.emit(
                        f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                        f"Checkpoint updated ({fetched} record(s) saved)."
                    )

                if len(batch) < request_size:
                    break

                skip += len(batch)

            if not self._is_running:
                all_submissions = self._sync_from_checkpoint()
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] Download cancelled. "
                    f"{len(all_submissions)} record(s) kept in checkpoint — "
                    "click Process Form to resume."
                )
                self.result.emit(all_submissions, False)
                self.finished.emit()
                return

            total_downloaded = len(all_submissions)
            if total_downloaded == 0:
                self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No submissions found.")
                if self.checkpoint:
                    self.checkpoint.clear()
            else:
                elapsed = self._format_duration(time.time() - start_time)
                self.log.emit(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Downloaded {total_downloaded} submission(s) in {page} page(s), {elapsed}."
                )

            self.progress.emit(100)
            self.status.emit(f"Downloaded {total_downloaded} submission(s)")
            if self.checkpoint and total_downloaded > 0:
                self.checkpoint.clear()
            self.result.emit(all_submissions, True)
            self.finished.emit()

        except requests.exceptions.Timeout as e:
            if self._emit_partial_failure(e):
                self.finished.emit()
                return
            self.error.emit(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                f"Timed out waiting for ODK Central after {self.read_timeout // 60} min. ({e})"
            )
            self.finished.emit()
        except requests.exceptions.RequestException as e:
            if self._emit_partial_failure(e):
                self.finished.emit()
                return
            self.error.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Error fetching submissions: {str(e)}")
            self.finished.emit()
        except Exception as e:
            if self._emit_partial_failure(e):
                self.finished.emit()
                return
            self.error.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Unexpected error: {str(e)}")
            self.finished.emit()




class ConnectODKDialog(QDialog, CollapsibleHelpMixin):
    """Dialog to get user input for ODK Central credentials and form selection."""
 

    # Add a validation method
    def validate_url(self):
        url = self.url_edit.text().strip()  # Remove leading/trailing spaces
        if not url.startswith("http://") and not url.startswith("https://"):
            QMessageBox.warning(self, "Invalid URL", "Please enter a valid URL (must start with http:// or https://).")
            return False
        return True
    

    def pre_login_with_validation(self):
        if not self.validate_url():
            return  # Exit if the URL is invalid
        self.pre_login()  # Proceed with the original login logic

    def strip_spaces(self):
        """Strip leading and trailing spaces on typing."""
        current_text = self.sender().text().strip()  # Get the text and strip spaces
        current_text = current_text.rstrip('/')  # Remove any trailing slashes
        self.sender().setText(current_text)  # Set the stripped text back
 
    """Dialog to get user input for ODK Central credentials and form selection."""

    def __init__(self, parent=None, default_url="", default_username="", default_password=""):
        """Constructor."""
        super().__init__(parent)
        configure_qgis_dialog(self, parent)

        self.settings = QSettings("AGS", "ODKConnect")

        self.setWindowTitle('Connector for ODK')
        self.setFixedSize(860, 450)

        # Initialize variables
        self.projects = []
        self.forms = []
        self.geo_data = []
        self.parent_entity_name = None

        # Create layout
        work_panel = QWidget()
        layout = QVBoxLayout(work_panel)
        form_layout = QFormLayout()

        # Create widgets
        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText("ODK Central URL")
        self.url_edit.setText(self.settings.value("url", default_url).strip())
        self.url_edit.textChanged.connect(self.strip_spaces)

        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("Username")
        self.username_edit.setText(self.settings.value("username", default_username))

        self.password_edit = QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.password_edit.setText(self.settings.value("password", default_password))

        self.save_button = QPushButton("Save Credentials")
        self.save_button.clicked.connect(self.save_credentials)

        self.project_combobox = QComboBox()
        self.form_combobox = QComboBox()
        self.form_combobox.currentIndexChanged.connect(self._update_download_options_visibility)
        self.filter_combobox = QComboBox()
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.pre_login_with_validation)

        self.process_button = QPushButton("Process Form")
        self.process_button.clicked.connect(self.pre_process_form)
        self.process_button.setEnabled(False)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_download)
        self.cancel_button.setEnabled(False)
        self.cancel_button.setToolTip("Stop the current submission download")

        self.csv_button = QPushButton("Get CSV")
        self.csv_button.clicked.connect(self.save_geojson_as_csv)
        self.csv_button.setEnabled(False)

        self.page_size_spinbox = QSpinBox()
        self.page_size_spinbox.setRange(10, 5000)
        self.page_size_spinbox.setSingleStep(50)
        self.page_size_spinbox.setValue(int(self.settings.value("page_size", ODK_PAGE_SIZE)))
        self.page_size_spinbox.setToolTip(
            "Number of submissions downloaded per request (default 100). "
            "Values above 250 may time out on busy servers; the plugin will "
            "auto-retry and split into smaller chunks; at high offsets uses batches of 5."
        )
        self.page_size_spinbox.valueChanged.connect(
            lambda value: self.settings.setValue("page_size", value)
        )

        self.read_timeout_spinbox = QSpinBox()
        self.read_timeout_spinbox.setRange(2, 120)
        self.read_timeout_spinbox.setSingleStep(5)
        self.read_timeout_spinbox.setSuffix(" min")
        self.read_timeout_spinbox.setValue(
            int(self.settings.value("read_timeout_min", ODK_READ_TIMEOUT // 60))
        )
        self.read_timeout_spinbox.setToolTip(
            "How long to wait for each server response. Increase this if your "
            "ODK Central nginx timeout has been raised. The collector gateway "
            "must also allow long requests or bulk pages will still return 504."
        )
        self.read_timeout_spinbox.valueChanged.connect(
            lambda value: self.settings.setValue("read_timeout_min", value)
        )

        self.download_all_checkbox = QCheckBox("Download all submissions at once")
        self.download_all_checkbox.setChecked(
            self.settings.value("download_all", True, type=bool)
        )
        self.download_all_checkbox.setToolTip(
            "Fetch every submission in a single request ($top=count&$expand=*). "
            "Increase read timeout and server nginx proxy_read_timeout for large forms."
        )
        self.download_all_checkbox.toggled.connect(self._on_download_all_toggled)

        self.paged_download_checkbox = QCheckBox("Download in pages instead")
        self.paged_download_checkbox.setChecked(
            not self.settings.value("download_all", True, type=bool)
        )
        self.paged_download_checkbox.setToolTip(
            "Use multiple smaller requests with checkpoint/resume support."
        )
        self.paged_download_checkbox.toggled.connect(self._on_paged_download_toggled)

        # Create the QGIS map canvas
        self.map_canvas = QgsMapCanvas()
        self.map_canvas.setCanvasColor(Qt.white)

        # Add widgets to form layout
        form_layout.addRow("ODK Central URL:", self.url_edit)
        form_layout.addRow("Username:", self.username_edit)
        form_layout.addRow("Password:", self.password_edit)
        form_layout.addRow("Project:", self.project_combobox)
        form_layout.addRow("Form:", self.form_combobox)
        self.page_size_label = QLabel("Download page size:")
        form_layout.addRow(self.page_size_label, self.page_size_spinbox)
        self.read_timeout_label = QLabel("Read timeout:")
        form_layout.addRow(self.read_timeout_label, self.read_timeout_spinbox)
        form_layout.addRow("", self.download_all_checkbox)
        form_layout.addRow("", self.paged_download_checkbox)
        self._update_download_options_visibility()

        # Create button layout
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.login_button)
        button_layout.addWidget(self.save_button)
        button_layout.addWidget(self.process_button)
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.csv_button)
        button_layout.addStretch()

        # Add progress bar
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setAlignment(Qt.AlignCenter)
        self.progress_bar.hide()

        # Add log window
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        self.log_textedit.setFixedHeight(100)
        self.log_textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        # Add clear log button
        self.clear_log_button = QPushButton("Clear Log")
        self.clear_log_button.clicked.connect(self.clear_log)

        credit_label = QLabel('''
            <div style="text-align: center;">
                <a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">Powered by ODK</a>
            </div>
        ''')
        credit_label.setAlignment(Qt.AlignCenter)
        credit_label.setOpenExternalLinks(True)

        disclaimer_label = QLabel('''
            <div style="text-align: center; font-size: 10px; color: gray;">
                <strong>Disclaimer:</strong> This plugin is not created, endorsed, or affiliated with ODK or its developers.
                For official resources, visit <a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">getodk.org</a>.
            </div>
        ''')
        disclaimer_label.setOpenExternalLinks(True)

        # Assemble layout
        layout.addLayout(form_layout)
        layout.addLayout(button_layout)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.log_textedit)
        layout.addWidget(self.clear_log_button)
        layout.addWidget(credit_label)
        layout.addWidget(disclaimer_label)

        self._attach_collapsible_help(work_panel, self._help_html(), add_toggle_row=False)
        button_layout.addWidget(self.toggle_help_button)
        self.submission_thread = None
        self.submission_worker = None
        self._download_cancelled = False

    @staticmethod
    def _help_html():
        return """
        <h3>Get Data</h3>
        <p>Connect to ODK Central, download form submissions, and load them as a map layer in QGIS.</p>

        <h4>Quick start</h4>
        <ol>
            <li>Enter your <b>ODK Central URL</b>, <b>username</b>, and <b>password</b>.</li>
            <li>Click <b>Login</b> to load projects and forms.</li>
            <li>Select a <b>project</b> and <b>form</b>.</li>
            <li>Click <b>Process Form</b> to fetch submissions and add a GeoJSON layer to the map.</li>
            <li>Use <b>Cancel</b> to stop a download in progress.</li>
            <li>Use <b>Get CSV</b> to export the processed data as a spreadsheet.</li>
        </ol>

        <h4>Credentials</h4>
        <p>Use <b>Save Credentials</b> to store your URL and login details for next time.</p>

        <h4>Download options</h4>
        <p>By default, all submissions are downloaded in <b>one request</b>. Increase <b>Read timeout</b> for large forms, and ensure your ODK Central server nginx timeout is high enough. Check <b>Download in pages instead</b> only if you need checkpoint/resume with multiple smaller requests.</p>

        <h4>Outputs</h4>
        <p>Submissions are converted to GeoJSON (EPSG:4326) and added to your QGIS project. A <code>submissions.json</code> file is also written to the working folder.</p>
        """

    @staticmethod
    def _checkpoint_base_dir():
        try:
            home = QgsProject.instance().homePath()
            if home:
                return Path(home) / ".connect_odk_checkpoints"
        except Exception:
            pass
        return Path(os.getcwd()) / ".connect_odk_checkpoints"

    def log_message(self, message):
        """Append a message to the log textedit widget."""
        self.log_textedit.append(message)
        self.log_textedit.ensureCursorVisible()

    def clear_log(self):
        """Clear all messages in the log window."""
        self.log_textedit.clear()

    def _on_download_all_toggled(self, checked):
        self.settings.setValue("download_all", checked)
        if checked and self.paged_download_checkbox.isChecked():
            self.paged_download_checkbox.blockSignals(True)
            self.paged_download_checkbox.setChecked(False)
            self.paged_download_checkbox.blockSignals(False)
        self._update_download_options_visibility()

    def _on_paged_download_toggled(self, checked):
        if checked:
            self.settings.setValue("download_all", False)
            if self.download_all_checkbox.isChecked():
                self.download_all_checkbox.blockSignals(True)
                self.download_all_checkbox.setChecked(False)
                self.download_all_checkbox.blockSignals(False)
        else:
            self.settings.setValue("download_all", True)
            if not self.download_all_checkbox.isChecked():
                self.download_all_checkbox.blockSignals(True)
                self.download_all_checkbox.setChecked(True)
                self.download_all_checkbox.blockSignals(False)
        self._update_download_options_visibility()

    def _update_download_options_visibility(self):
        """Show download options only after a form has been selected."""
        has_form = (
            self.form_combobox.isEnabled()
            and self.form_combobox.count() > 0
            and bool(self.form_combobox.currentText().strip())
        )
        paged = self.paged_download_checkbox.isChecked()
        self.download_all_checkbox.setVisible(has_form)
        self.paged_download_checkbox.setVisible(has_form)
        self.page_size_label.setVisible(has_form and paged)
        self.page_size_spinbox.setVisible(has_form and paged)
        self.read_timeout_label.setVisible(has_form)
        self.read_timeout_spinbox.setVisible(has_form)

    def _set_download_active(self, active):
        """Enable or disable controls while a submission download is running."""
        self.cancel_button.setEnabled(active)
        self.process_button.setEnabled(not active and self.form_combobox.count() > 0)
        self.login_button.setEnabled(not active)
        self.save_button.setEnabled(not active)
        self.page_size_spinbox.setEnabled(not active)
        self.read_timeout_spinbox.setEnabled(not active)
        self.download_all_checkbox.setEnabled(not active)
        self.paged_download_checkbox.setEnabled(not active)

    def cancel_download(self):
        """Cancel an in-progress submission download."""
        if not self.submission_worker:
            return
        self._download_cancelled = True
        self.log_message(
            f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
            "Cancel requested — stopping after the current page finishes..."
        )
        self.update_progress_status("Cancelling...")
        self.submission_worker.stop()

    def pre_process_form(self):
        """Start submission fetching in a background thread with immediate UI feedback."""
        self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Starting form processing...")
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting download...")
        self.progress_bar.show()
        QApplication.processEvents()  # Force UI update

        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
        selected_project_name = self.project_combobox.currentText()
        selected_form_name = self.form_combobox.currentText()

        selected_project_id = None
        for project in self.projects:
            if project['name'] == selected_project_name:
                selected_project_id = project['id']
                break

        if not selected_project_id:
            self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No project selected.")
            self.progress_bar.hide()
            return

        try:
            form_id = self.get_form_id_from_name(selected_form_name, selected_project_id)
        except Exception as e:
            self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Error: {str(e)}")
            self.progress_bar.hide()
            return

        self._download_cancelled = False
        self._set_download_active(True)

        if self.submission_thread and self.submission_thread.isRunning():
            self.log_message(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                "A download is already running."
            )
            self._set_download_active(False)
            return

        self.submission_thread = QThread()
        checkpoint_dir = DownloadCheckpoint.get_checkpoint_dir(
            self._checkpoint_base_dir(),
            selected_project_id,
            form_id,
        )
        self.submission_worker = SubmissionWorker(
            server_url,
            username,
            password,
            selected_project_id,
            form_id,
            page_size=self.page_size_spinbox.value(),
            checkpoint_dir=checkpoint_dir,
            read_timeout=self.read_timeout_spinbox.value() * 60,
            download_all=self.download_all_checkbox.isChecked(),
        )
        self.submission_worker.moveToThread(self.submission_thread)
        self.submission_worker.progress.connect(self.update_progress)
        self.submission_worker.status.connect(self.update_progress_status)
        self.submission_worker.log.connect(self.log_message)
        self.submission_worker.result.connect(self.on_submissions_fetched)
        self.submission_worker.error.connect(self.on_submission_error)
        self.submission_worker.finished.connect(self.on_submission_finished)
        self.submission_thread.started.connect(self.submission_worker.run)
        self.submission_thread.start()

    def update_progress(self, value):
        """Update progress bar value."""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)

    def update_progress_status(self, text):
        """Update progress bar label text."""
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat(text)
        QApplication.processEvents()

    def on_submissions_fetched(self, submissions, complete=True):
        """Handle fetched submissions and continue processing."""
        if not complete:
            count = len(submissions)
            if self._download_cancelled:
                self.log_message(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"Download cancelled. {count} record(s) saved to checkpoint — "
                    "click Process Form to resume."
                )
            elif count:
                self.log_message(
                    f"[{datetime.now().strftime('%H:%M:%S.%f')}] "
                    f"{count} record(s) saved to checkpoint — click Process Form to resume."
                )
            return

        if self._download_cancelled:
            return

        try:
            if not submissions:
                self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No submissions found.")
                QMessageBox.warning(self, "No Submissions", "No submissions found for the selected form.")
                return

            # Store parent entity name for layer naming
            if hasattr(self, 'parent_combo') and self.parent_combo.currentText():
                self.parent_entity_name = self.parent_combo.currentText()
            else:
                self.parent_entity_name = "data"

            with open('submissions.json', 'w') as f:
                json.dump(submissions, f, indent=2)
                self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Submissions saved to submissions.json")

            geojson_data = self.convert_to_geojson(submissions, 'out.json')
            self.add_geojson_to_map(geojson_data, self.form_combobox.currentText())

        except Exception as e:
            self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Error processing submissions: {str(e)}")
            QMessageBox.critical(self, "Error", f"Error processing form: {str(e)}")

    def on_submission_error(self, error_message):
        """Handle errors from submission worker."""
        if self._download_cancelled:
            return
        self.log_message(error_message)
        QMessageBox.critical(self, "Error", error_message.split("] ")[-1])

    def on_submission_finished(self):
        """Clean up after submission worker finishes."""
        self.progress_bar.hide()
        self.progress_bar.setFormat("")
        self._set_download_active(False)
        if self.submission_thread:
            self.submission_thread.quit()
            self.submission_thread.wait()
        if self.submission_worker:
            self.submission_worker.deleteLater()
            self.submission_worker = None
        self.submission_thread = None
        self._download_cancelled = False

    def closeEvent(self, event):
        """Handle dialog close event to clean up threads."""
        if self.submission_thread and self.submission_thread.isRunning():
            if self.submission_worker:
                self.submission_worker.stop()
                self.submission_worker.deleteLater()
            self.submission_thread.quit()
            self.submission_thread.wait()
        super().closeEvent(event)


 

    def get_form_data(self):
        """Return the form data entered by the user."""
        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
        selected_project = self.project_combobox.currentText()
        selected_form = self.form_combobox.currentText()
        return server_url, username, password, selected_project, selected_form

    def set_projects_and_forms(self, projects, forms=[]):
        """Set the available projects and forms in the comboboxes."""
        self.project_combobox.clear()
        self.project_combobox.addItems([project['name'] for project in projects])
        
        # Clear and disable form combobox until a project is selected
        self.form_combobox.clear()
        self.form_combobox.setEnabled(True)
        self._update_download_options_visibility()

    def pre_login(self):
        """start progress bar"""
        self.progress_bar.show()
        # Use QTimer to delay the login function by 1 second (1000 milliseconds)
        QTimer.singleShot(1000, self.login)


    def login(self):
        """Login to ODK Central and fetch projects and forms."""
        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
      
        # Fetch projects
        try:
            
            projects = self.fetch_projects(server_url, username, password)
            self.projects = projects  # Store the fetched projects
            # Initially hide the progress bar
            self.progress_bar.hide()

            # Populate the project combobox
            self.set_projects_and_forms(projects)

            # Enable the project combobox and disable the form combobox until a project is selected
            self.project_combobox.setEnabled(True)
            self.form_combobox.setEnabled(True)

            # Automatically select the first project (index 0)
            self.project_combobox.setCurrentIndex(0)

            # Trigger the on_project_selected method manually after setting the index
            
            self.on_project_selected()

            # Connect the signal when a project is selected to fetch forms
            self.project_combobox.currentIndexChanged.connect(self.on_project_selected)

        except Exception as e:
            # Display error message to the user
            error_message = f"Error fetching projects: {str(e)}"
            QMessageBox.critical(self, "Login Error", error_message)
            self.progress_bar.hide()

            # Optionally, you can also raise the exception if you want to propagate it further
            #raise

 
    def on_project_selected(self):
        """Fetch forms when a project is selected."""
        selected_project_name = self.project_combobox.currentText()

        
        # Find the project ID from the list of projects
        selected_project_id = None
        for project in self.projects:
            if project['name'] == selected_project_name:
                selected_project_id = project['id']
                break

        if selected_project_id:
            try:
                # Fetch forms for the selected project
 
         
                forms = self.fetch_forms(self.url_edit.text(), self.username_edit.text(), self.password_edit.text(), selected_project_id)
                 
                # Store the forms in self.forms
                self.forms = forms  # Store the fetched forms

                # Populate the form combobox
                self.form_combobox.clear()
                self.form_combobox.addItems([form['name'] for form in forms])
                self.form_combobox.setEnabled(True)

                # Enable Process Form button after form selection
                self.process_button.setEnabled(True)
                self._update_download_options_visibility()

            except Exception as e:
                print(f"Error fetching forms: {str(e)}")
                self.form_combobox.setEnabled(False)
                self._update_download_options_visibility()

    def fetch_projects(self, server_url, username, password):
        """Fetch projects from ODK Central."""
        
        projects_api_url = f"{server_url}/v1/projects"
        
        try:
            response = requests.get(
                projects_api_url,
                auth=(username, password),
                timeout=(ODK_CONNECT_TIMEOUT, ODK_READ_TIMEOUT),
            )
            response.raise_for_status()
            projects = response.json()
            #self.progress_bar.hide()
            return projects
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching projects: {str(e)}")

    def fetch_forms(self, server_url, username, password, project_id):
        """Fetch forms for the selected project."""
        forms_api_url = f"{server_url}/v1/projects/{project_id}/forms"
        try:
            response = requests.get(
                forms_api_url,
                auth=(username, password),
                timeout=(ODK_CONNECT_TIMEOUT, ODK_READ_TIMEOUT),
            )
            response.raise_for_status()
            forms = response.json()
            return forms
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching forms: {str(e)}")

    def get_form_id_from_name(self, form_name, project_id):
        """Helper function to get the form ID based on the form name."""

        
        if not self.forms:
            raise Exception("No forms available. Please select a project.")
        
        for form in self.forms:
            if form['name'] == form_name:
                return form['xmlFormId']
        
        raise Exception(f"Form ID not found for form: {form_name}")

 
    def hide_progress(self):
      """Hide progress bar"""
      self.progress_bar.hide()
 
 

    def find_geometry(self, data):
        """
        Recursively search for GeoJSON geometry in the data.
        :param data: Dictionary that might contain GeoJSON geometry
        :return: The GeoJSON geometry (or None if not found)
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    if 'type' in value and 'coordinates' in value:
                        return value
                    geometry = self.find_geometry(value)
                    if geometry:
                        return geometry
        elif isinstance(data, list):
            for item in data:
                geometry = self.find_geometry(item)
                if geometry:
                    return geometry
        return None

    def flatten_properties(self, d):
        """
        Flatten a nested dictionary to extract leaf nodes only.
        :param d: Dictionary to flatten
        :return: Flattened dictionary
        """
        leaves = {}
        for key, value in d.items():
            if isinstance(value, dict):
                leaves.update(self.flatten_properties(value))
            elif not isinstance(value, list):  # Skip lists
                leaves[key] = value
        return leaves

    def convert_to_geojson(self, data_array, output_file):
        """
        Convert a list of data dictionaries into a GeoJSON FeatureCollection,
        handling cases with and without nesting, with 5 decimal precision and EPSG:4326 CRS.
        
        :param data_array: List of dictionaries containing 'geometry' and 'properties'
        :param output_file: The output file to save the GeoJSON data
        :return: GeoJSON FeatureCollection
        """
        features = []

        def round_coordinates(geometry):
            """Recursively round coordinates to 5 decimal places."""
            if isinstance(geometry, dict) and 'coordinates' in geometry:
                if isinstance(geometry['coordinates'], list):
                    geometry['coordinates'] = [
                        [
                            round(c, 5) if isinstance(c, (int, float)) else c
                            for c in coords
                        ] if isinstance(coords, list) else round(coords, 5)
                        for coords in geometry['coordinates']
                    ]
            return geometry

        for data in data_array:
            # Flatten all parent-level properties
            parent_properties = self.flatten_properties(data)
            found_geometry = self.find_geometry(data)

            # If geometry is found at the root level, create a feature
            if found_geometry:
                found_geometry = round_coordinates(found_geometry)
                geojson_feature = {
                    "type": "Feature",
                    "geometry": found_geometry,
                    "properties": parent_properties
                }
                features.append(geojson_feature)
                continue

            # If no root-level geometry, look for nested data structures
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        # Flatten each nested structure and find geometry
                        nested_geometry = self.find_geometry(item)
                        nested_properties = self.flatten_properties(item)

                        # Combine parent properties with nested properties
                        combined_properties = {**parent_properties, **nested_properties}

                        if nested_geometry:
                            nested_geometry = round_coordinates(nested_geometry)
                            geojson_feature = {
                                "type": "Feature",
                                "geometry": nested_geometry,
                                "properties": combined_properties
                            }
                            features.append(geojson_feature)

        # Create a GeoJSON FeatureCollection with CRS
        geojson_collection = {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:EPSG::4326"
                }
            },
            "features": features
        }

        # Save GeoJSON data to the specified file
        with open(output_file, 'w') as f:
            json.dump(geojson_collection, f, indent=2)

        print(f"GeoJSON data saved to {output_file}")
        self.csv_button.setEnabled(True)

        return geojson_collection
    
    
        
    def remove_empty_properties(self,geojson_data):
        """Remove empty properties from GeoJSON features."""
        for feature in geojson_data.get('features', []):
            # Filter out empty properties for each feature
            feature['properties'] = {key: value for key, value in feature['properties'].items() if value not in [None, '', [], {}, {}, False]}
        return geojson_data
 

 
    def add_geojson_to_map(self, geojson_data, form_name):
        """Add GeoJSON data as separate layers to the map based on geometry type.
        Saves data to Documents folder and loads as layers with appropriate names."""
        
        # Remove empty properties
        geojson_data = self.remove_empty_properties(geojson_data)
        self.geo_data = geojson_data

        # Get Documents folder path
        documents_path = Path.home() / "Documents"
        if not documents_path.exists():
            documents_path = Path.home() / "My Documents"
        
        # Create ODK folder in Documents if it doesn't exist
        odk_folder = documents_path / "ODK_Data"
        odk_folder.mkdir(exist_ok=True)

        # Split features by geometry type
        geometry_types = {
            "Point": [],
            "Linear": [],
            "Polygon": [],
        }

        # Separate features by geometry type
        for feature in geojson_data.get("features", []):
            geometry_type = feature["geometry"]["type"]
            if geometry_type in ["LineString", "MultiLineString"]:
                geometry_types["Linear"].append(feature)
            elif geometry_type in ["Point", "MultiPoint"]:
                geometry_types["Point"].append(feature)
            elif geometry_type in ["Polygon", "MultiPolygon"]:
                geometry_types["Polygon"].append(feature)
            else:
                # For any other geometry types, add to Linear as fallback
                geometry_types["Linear"].append(feature)
        
        # Create layers for each geometry type
        for geom_type, features in geometry_types.items():
            if not features:
                continue  # Skip if no features for this geometry type
            
            # Create a GeoJSON string for this geometry type
            geom_geojson_data = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {
                        "name": "urn:ogc:def:crs:EPSG::4326"
                    }
                },
                "features": features
            }
            
            # Save to Documents folder
            try:
                # Create filename with timestamp and parent entity info
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Get parent entity name if available
                parent_entity = "data"
                if hasattr(self, 'parent_combo') and self.parent_combo.currentText():
                    parent_entity = self.parent_combo.currentText()
                elif hasattr(self, 'parent_entity_name') and self.parent_entity_name:
                    parent_entity = self.parent_entity_name
                
                # Create descriptive filename
                filename = f"{form_name}_{parent_entity}_{geom_type}_{timestamp}.geojson"
                file_path = odk_folder / filename
                
                # Write GeoJSON data to file
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(geom_geojson_data, f, indent=2)
                
                self.log_message(f"Saved {geom_type} layer to: {file_path}")
                
                # Create layer from file
                layer_name = f"{form_name}_{parent_entity}_{geom_type}"
                vector_layer = QgsVectorLayer(str(file_path), layer_name, "ogr")
                
                if vector_layer.isValid():
                    # Set CRS explicitly
                    vector_layer.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
                    
                    # Add the vector layer to the current map project
                    QgsProject.instance().addMapLayer(vector_layer)
                    
                    self.log_message(f"Successfully loaded {len(features)} {geom_type} features as layer: {layer_name}")
                else:
                    self.log_message(f"Failed to load {geom_type} layer from file: {file_path}")
                        
            except Exception as e:
                self.log_message(f"Error creating {geom_type} layer: {str(e)}")
        
        # Optionally zoom to the extent of all added layers
        self.map_canvas.zoomToFullExtent()
        self.map_canvas.refresh()
        self.hide_progress()

        self.log_message("GeoJSON data has been saved to Documents/ODK_Data folder and loaded as layers.")
        self.log_message(f"Files saved to: {odk_folder}")

    def extract_headers_from_geojson(self,features):
        """
        Extract all unique property keys from GeoJSON features in the order they are encountered.

        :param features: List of GeoJSON features.
        :return: List of unique headers including geometry fields.
        """
        headers = OrderedDict()  # Use OrderedDict to preserve order
        for feature in features:
            if isinstance(feature, dict) and "properties" in feature:
                for key in feature["properties"].keys():
                    headers[key] = None  # Add keys in order of their first appearance

        # Add geometry fields to the headers
        return list(headers.keys()) + ["latitude", "longitude"]

    def save_geojson_as_csv(self):
        """
        Save GeoJSON data as a CSV file.

        :param self: Reference to the plugin instance.
        """
        try:
            # Ensure GeoJSON data is a dictionary
            geo = self.geo_data
            QgsMessageLog.logMessage("Starting the process to save GeoJSON as CSV...", "GeoJSON to CSV")

            if isinstance(geo, str):
                try:
                    geo = json.loads(geo)
                except json.JSONDecodeError:
                    QMessageBox.warning(self, "Error", "Invalid GeoJSON string.")
                    return
            elif not isinstance(geo, dict):
                raise ValueError("GeoJSON data must be a dictionary or a valid JSON string.")

            if "features" not in geo:
                QMessageBox.warning(self, "Error", "GeoJSON data is missing 'features' key.")
                return

            features = geo.get("features", [])
            if not features:
                QMessageBox.warning(self, "Error", "No features found in GeoJSON.")
                return

            # Extract headers in order
            headers = self.extract_headers_from_geojson(features)

            # Prompt user for file location
            output_file, _ = QFileDialog.getSaveFileName(
                self, "Save CSV File", "", "CSV Files (*.csv);;All Files (*)"
            )
            if not output_file:
                QMessageBox.warning(self, "Cancelled", "No file selected.")
                return
            if not output_file.endswith(".csv"):
                output_file += ".csv"

            # Write to CSV
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

                for feature in features:
                    if isinstance(feature, dict):
                        row = feature.get("properties", {}).copy()

                        # Add geometry fields for point geometries
                        geometry = feature.get("geometry", {})
                        if geometry and geometry.get("type", "") == "Point":
                            coordinates = geometry.get("coordinates", [])
                            if len(coordinates) >= 2:
                                row["latitude"] = coordinates[1]
                                row["longitude"] = coordinates[0]
                            else:
                                row["latitude"] = None
                                row["longitude"] = None
                        else:
                            row["latitude"] = None
                            row["longitude"] = None

                        writer.writerow(row)

            QgsMessageLog.logMessage(f"CSV successfully saved to {output_file}", "GeoJSON to CSV")
            QMessageBox.information(self, "Success", f"CSV saved to {output_file}")

        except Exception as e:
            QgsMessageLog.logMessage(f"Error occurred: {str(e)}", "GeoJSON to CSV")
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")

    def save_credentials(self):
        """Save the entered credentials."""
        # Get the entered values from the text fields
        url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()

        # Save them to QSettings
        self.settings.setValue("url", url)
        self.settings.setValue("username", username)
        self.settings.setValue("password", password)

        # Show a confirmation message
        QMessageBox.information(self, "Success", "Credentials saved successfully!")

        # Optionally, print or log the saved values for debugging (do not do this for passwords in production)
        print(f"Saved URL: {url}, Username: {username}")
