import psutil
import time
import os
import threading


class DynamicProcessMonitor:
    def __init__(self, base_interval=1.0):
        self.parent_pid = os.getpid()
        self.base_interval = base_interval
        self.running = False
        self.monitor_thread = None
        self.stats = []
        self.file_history = set()
        self._lock = threading.Lock()

    def start(self):
        """Start continuous base monitoring"""
        if self.running:
            return
        self.running = True

        def monitor_loop():
            while self.running:
                with self._lock:
                    self._snapshot('base')

                interval = self.base_interval
                time.sleep(interval)

        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()

    def high_freq_snapshot(self, function_name, interval):
        """Take IMMEDIATE high-frequency snapshot during monitoring"""
        self.base_interval = interval
        with self._lock:
            return self._snapshot('high_freq', function_name)

    def _snapshot(self, snapshot_type, function_name):
        """Internal snapshot method"""
        snapshot = {
            'timestamp': time.time(),
            'type': snapshot_type,
            'processes': {}
        }

        newly_opened_files = []

        # Parent process
        try:
            parent_proc = psutil.Process(self.parent_pid)
            open_files_list = parent_proc.open_files()

            snapshot['processes'][self.parent_pid] = {
                'rss_mb': parent_proc.memory_info().rss / 1024 / 1024,
                'open_files_count': len(open_files_list)
            }

            # Only track NEWLY opened files
            current_files = {f.path for f in open_files_list}
            newly_opened_files = sorted(current_files - self.file_history)
            if snapshot_type == 'high_freq':
                self.file_history.update(current_files)

        except psutil.NoSuchProcess:
            pass

        # Child processes (track new files too)
        try:
            for child in psutil.Process().children(recursive=True):
                child_open_files = child.open_files()
                snapshot['processes'][child.pid] = {
                    'rss_mb': child.memory_info().rss / 1024 / 1024,
                    'open_files_count': len(child_open_files)
                }
        except psutil.NoSuchProcess:
            pass

        # NEWLY opened files list (across all processes)
        snapshot['newly_opened_files'] = newly_opened_files

        snapshot['total_rss_mb'] = sum(p['rss_mb'] for p in snapshot['processes'].values())
        snapshot['process_count'] = len(snapshot['processes'])
        snapshot['function_name'] = function_name
        self.stats.append(snapshot)
        return snapshot

    def stop(self):
        """Stop continuous monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)

    def get_stats(self):
        """Get all collected data"""
        return self.stats.copy()

    def clear(self):
        """Reset all data"""
        self.stats.clear()
        self.file_history.clear()
