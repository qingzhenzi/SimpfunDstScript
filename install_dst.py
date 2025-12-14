#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
import urllib.request
import urllib.error
import tarfile
import re
import time
import json
import traceback
import shutil

# ================= 默认配置 =================
# 建议显式指定目录，不要依赖默认值
DEFAULT_ROOT_DIR = "/home/container/games"
DEFAULT_GAME_DIR = os.path.join(DEFAULT_ROOT_DIR, "dst")
DEFAULT_STEAMCMD_DIR = os.path.join(DEFAULT_ROOT_DIR, "steamcmd")

APP_ID = "343050"
DEFAULT_STEAMCMD_URL = "https://steamcdn-a.akamaihd.net/client/installer/steamcmd_linux.tar.gz"
VALIDATION_BINARY = "bin/dontstarve_dedicated_server_nullrenderer"
MAX_RETRIES = 5
RETRY_DELAY = 10
MIN_DISK_SPACE_MB = 2048

ERR_UNKNOWN = "ERR_UNKNOWN"
ERR_DEPENDENCY = "ERR_DEPENDENCY"
ERR_NETWORK = "ERR_NETWORK"
ERR_DISK = "ERR_DISK"
ERR_PERMISSION = "ERR_PERMISSION"
ERR_STEAMCMD = "ERR_STEAMCMD"

class Logger:
    def __init__(self, json_mode):
        self.json_mode = json_mode
    
    def info(self, msg):
        if self.json_mode:
            sys.stderr.write(f"[INFO] {msg}\n")
        else:
            print(f"[INFO] {msg}")

    def warn(self, msg):
        sys.stderr.write(f"[WARN] {msg}\n") if self.json_mode else print(f"[WARN] {msg}")

    def error(self, msg):
        sys.stderr.write(f"[ERROR] {msg}\n")

    def output_json(self, data):
        print(json.dumps(data, indent=None if self.json_mode else 4))

class DSTManager:
    def __init__(self, args, logger):
        self.logger = logger
        self.game_dir = os.path.abspath(args.install_dir)
        self.steamcmd_dir = os.path.abspath(args.steamcmd_dir)
        self.steamcmd_exe = os.path.join(self.steamcmd_dir, "steamcmd.sh")
        self.manifest_path = os.path.join(self.game_dir, "steamapps", f"appmanifest_{APP_ID}.acf")
        self.steamcmd_url = args.steamcmd_url
        self.force = args.force
        self.env = os.environ.copy()
        self.proxy_url = args.proxy
        self.download_total_bytes = 0 
        
        if args.proxy:
            self.env["http_proxy"] = args.proxy
            self.env["https_proxy"] = args.proxy
            self.env["all_proxy"] = args.proxy

    def _ensure_dir(self, path):
        if not os.path.exists(path):
            try:
                os.makedirs(path, exist_ok=True)
            except PermissionError:
                raise OSError(ERR_PERMISSION, f"No write permission: {path}")

    def _check_disk_space(self, path):
        if not os.path.exists(path):
            path = os.path.dirname(path)
        try:
            total, used, free = shutil.disk_usage(path)
            if (free // (1024*1024)) < MIN_DISK_SPACE_MB:
                raise OSError(ERR_DISK, f"Insufficient disk space. Free: {free//1024//1024}MB")
        except:
            pass

    def prepare_steamcmd(self):
        self._ensure_dir(self.steamcmd_dir)
        if os.path.exists(self.steamcmd_exe): return
        
        self.logger.info("Installing SteamCMD...")
        try:
            tar_path = os.path.join(self.steamcmd_dir, "steamcmd.tar.gz")
            opener = urllib.request.build_opener()
            if self.proxy_url:
                opener.add_handler(urllib.request.ProxyHandler({'http':self.proxy_url, 'https':self.proxy_url}))
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(self.steamcmd_url, tar_path)
            
            # === 修复 DeprecationWarning ===
            with tarfile.open(tar_path, "r:gz") as tar:
                # Python 3.12+ 推荐 filter='data'，老版本不支持 filter 参数需做兼容
                if sys.version_info >= (3, 12):
                    tar.extractall(path=self.steamcmd_dir, filter='data')
                else:
                    tar.extractall(path=self.steamcmd_dir)
                    
            os.remove(tar_path)
            os.chmod(self.steamcmd_exe, 0o755)
        except Exception as e:
            raise OSError(ERR_NETWORK, f"Failed to install SteamCMD: {e}")

    def get_local_version(self):
        if not os.path.exists(self.manifest_path): return "0"
        try:
            with open(self.manifest_path, 'r') as f:
                match = re.search(r'"buildid"\s+"(\d+)"', f.read())
                return match.group(1) if match else "0"
        except: return "0"

    def get_remote_version(self):
        self.logger.info("Querying remote version...")
        cmd = [self.steamcmd_exe, "+login", "anonymous", "+app_info_update", "1", "+app_info_print", APP_ID, "+quit"]
        try:
            # 使用 universal_newlines=True 兼容性更好
            res = subprocess.run(
                cmd, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                universal_newlines=True, check=False, timeout=60
            )
            pub = re.search(r'"branches"\s*\{.*?"public"\s*\{(.*?)\}', res.stdout, re.DOTALL)
            if pub:
                bid = re.search(r'"buildid"\s+"(\d+)"', pub.group(1))
                if bid: return bid.group(1)
            return None
        except Exception: return None

    def run_update_process(self):
        self._ensure_dir(self.game_dir)
        self._check_disk_space(self.game_dir)
        
        cmd = [
            self.steamcmd_exe,
            "+force_install_dir", self.game_dir,
            "+login", "anonymous",
            "+app_update", APP_ID, "validate",
            "+quit"
        ]

        progress_pattern = re.compile(r'progress:\s+\d+\.\d+\s+\(\d+\s+/\s+(\d+)\)')

        success = False
        last_error = ""

        for attempt in range(1, MAX_RETRIES + 1):
            self.logger.info(f"Update attempt {attempt}/{MAX_RETRIES}...")
            
            try:
                process = subprocess.Popen(
                    cmd, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                    universal_newlines=True
                )
                
                output_buffer = []
                while True:
                    line = process.stdout.readline()
                    if not line: break
                    line = line.strip()
                    output_buffer.append(line)
                    
                    if "downloading" in line:
                        match = progress_pattern.search(line)
                        if match:
                            total_bytes = int(match.group(1))
                            if total_bytes > self.download_total_bytes:
                                self.download_total_bytes = total_bytes
                    
                    if line and not self.logger.json_mode:
                        print(f"    {line}")

                process.wait()
                full_log = "\n".join(output_buffer)

                if process.returncode == 0 and "Success! App '343050' fully installed" in full_log:
                    success = True
                    break
                else:
                    last_error = f"SteamCMD failed (Code {process.returncode})"
                    if "0x202" in full_log: raise OSError(ERR_NETWORK, "Rate Limit/Network (0x202)")
                    if "0x6" in full_log: raise OSError(ERR_DISK, "Disk Write Fail (0x6)")

            except Exception as e:
                last_error = str(e)
                self.logger.warn(f"Attempt {attempt} error: {e}")
                time.sleep(RETRY_DELAY)
        
        if not success:
            raise OSError(ERR_STEAMCMD, last_error)

    def execute(self):
        try:
            self.prepare_steamcmd()
            old_ver = self.get_local_version()
            remote_ver = self.get_remote_version()
            
            should_update = False
            status = "up_to_date"

            if self.force:
                should_update = True
            elif remote_ver is None:
                should_update = True
            elif old_ver != remote_ver:
                should_update = True
                self.logger.info(f"Update available: {old_ver} -> {remote_ver}")

            if should_update:
                self.run_update_process()
                new_ver = self.get_local_version()
                status = "fresh_installed" if old_ver == "0" else "updated"
            else:
                new_ver = old_ver

            size_mb = round(self.download_total_bytes / (1024 * 1024), 2)

            return {
                "status": "success",
                "state": status,
                "version_info": {
                    "old_version": old_ver,
                    "new_version": new_ver,
                    "remote_version": remote_ver or "unknown"
                },
                "download_info": {
                    "total_bytes": self.download_total_bytes,
                    "total_mb": size_mb,
                    "note": "Size captured from SteamCMD log"
                },
                "paths": {
                    "install_dir": self.game_dir,
                    "steamcmd_dir": self.steamcmd_dir
                },
                "timestamp": int(time.time())
            }

        except OSError as e:
            code = e.args[0] if len(e.args) > 1 else ERR_UNKNOWN
            msg = e.args[1] if len(e.args) > 1 else str(e)
            raise RuntimeError(json.dumps({"code": code, "message": msg}))
        except Exception as e:
            raise RuntimeError(json.dumps({"code": ERR_UNKNOWN, "message": str(e)}))

def main():
    parser = argparse.ArgumentParser(description="DST Manager")
    parser.add_argument("--install-dir", default=DEFAULT_GAME_DIR)
    parser.add_argument("--steamcmd-dir", default=DEFAULT_STEAMCMD_DIR)
    parser.add_argument("--proxy", help="http://ip:port")
    parser.add_argument("--steamcmd-url", default=DEFAULT_STEAMCMD_URL)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--force", action="store_true")
    
    args = parser.parse_args()
    logger = Logger(args.json)
    
    try:
        manager = DSTManager(args, logger)
        result = manager.execute()
        logger.output_json(result)
    except RuntimeError as e:
        try:
            err = json.loads(str(e))
        except:
            err = {"code": ERR_UNKNOWN, "message": str(e)}
        logger.output_json({"status": "error", "error": err})
        sys.exit(1)

if __name__ == "__main__":
    main()