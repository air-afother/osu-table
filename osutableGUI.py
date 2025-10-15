import json
import sqlite3
import requests
import os
import re
import zipfile
import threading
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import webbrowser

# === CONFIG ===
SONGDATA_DB = "songdata.db"
JSON_URLS = {
    "7K + 8K": "https://air-afother.github.io/osu-table/osu_mania_7k_8k_final.json",
    "4K": "https://air-afother.github.io/osu-table/osu_mania_4k_final.json"
}
TABLE_URLS = {
    "7K + 8K": "https://air-afother.github.io/osu-table/",
    "4K": "https://air-afother.github.io/osu-table/index4k.html"
}
NERINYAN_BASE = "https://api.nerinyan.moe/d/"
HEADERS = {"User-Agent": "osu-downloader/1.0"}


# === Helper functions ===

def get_existing_md5():
    """Return set of md5 hashes present in local song database."""
    if not os.path.exists(SONGDATA_DB):
        messagebox.showerror("Error", f"Database not found: {SONGDATA_DB}")
        return set()

    conn = sqlite3.connect(SONGDATA_DB)
    cur = conn.cursor()
    cur.execute("SELECT md5 FROM song")
    md5_list = {row[0] for row in cur.fetchall()}
    conn.close()
    return md5_list


def get_all_maps(url):
    """Download and return JSON with all maps from given URL."""
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def extract_beatmapset_id(url: str):
    """Extract beatmapset ID from osu URL."""
    match = re.search(r"beatmapsets/(\d+)", url)
    return match.group(1) if match else None


def sanitize_filename(name: str):
    """Remove invalid characters from filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", name)


def download_missing_maps(missing_maps, download_root, progress_callback):
    """
    Download missing maps from nerinyan. For each map, progress_callback(done, total, start_time) is called.
    """
    os.makedirs(download_root, exist_ok=True)
    total = len(missing_maps)
    done = 0
    start_time = time.time()

    for m in missing_maps:
        beatmapset_id = extract_beatmapset_id(m["url"])
        if not beatmapset_id:
            done += 1
            progress_callback(done, total, start_time)
            continue

        download_url = f"{NERINYAN_BASE}{beatmapset_id}"
        filename = f"{sanitize_filename(m['title'])} - {sanitize_filename(m['artist'])} [{beatmapset_id}].osz"
        filepath = os.path.join(download_root, filename)

        # skip existing file
        if os.path.exists(filepath):
            done += 1
            progress_callback(done, total, start_time)
            continue

        try:
            with requests.get(download_url, stream=True, headers=HEADERS, timeout=30) as r:
                r.raise_for_status()
                total_length = int(r.headers.get('content-length', 0) or 0)
                # guard small responses (likely error/HTML)
                if total_length and total_length < 200_000:
                    done += 1
                    progress_callback(done, total, start_time)
                    continue

                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception:
            # on failure, skip (we still count it to avoid stalling)
            pass

        done += 1
        progress_callback(done, total, start_time)


def extract_osz_files(download_root, delete_after=False):
    """Extract .osz archives under download_root and optionally delete the .osz files."""
    if not os.path.isdir(download_root):
        return
    osz_files = [f for f in os.listdir(download_root) if f.lower().endswith(".osz")]
    for osz_file in osz_files:
        osz_path = os.path.join(download_root, osz_file)
        extract_folder = os.path.join(download_root, os.path.splitext(osz_file)[0])
        try:
            with zipfile.ZipFile(osz_path, "r") as zip_ref:
                zip_ref.extractall(extract_folder)
        except Exception:
            pass

    if delete_after:
        for osz_file in osz_files:
            try:
                os.remove(os.path.join(download_root, osz_file))
            except Exception:
                pass


# === Main background task ===

def start_download(selected_tables, level_ranges_int, auto_extract, output_path, progress_bar, status_label, start_button, count_label):
    """
    selected_tables: list of table names (keys of JSON_URLS)
    level_ranges_int: dict name -> (min_int, max_int)
    """
    def task():
        start_button.config(state=tk.DISABLED)
        progress_bar["value"] = 0
        progress_bar.update()
        status_label.config(text="Preparing...")
        count_label.config(text="0/0")

        existing_md5 = get_existing_md5()
        if not existing_md5:
            start_button.config(state=tk.NORMAL)
            return

        # collect maps from selected tables and filter by integer range (min <= level <= max)
        combined_maps = []
        for table in selected_tables:
            url = JSON_URLS.get(table)
            if not url:
                continue
            try:
                maps = get_all_maps(url)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load map list for {table}:\n{e}")
                start_button.config(state=tk.NORMAL)
                return

            min_i, max_i = level_ranges_int.get(table, (0, 20))
            # levels are floats like 3, 3.5, 4; we include those between min_i and max_i inclusive
            for m in maps:
                lvl_str = m.get("level")
                try:
                    lvl = float(lvl_str)
                except Exception:
                    continue
                if min_i <= lvl <= max_i:
                    combined_maps.append(m)

        # deduplicate by md5 (keep first)
        seen_md5 = set()
        unique_maps = []
        for m in combined_maps:
            md = m.get("md5")
            if not md or md in seen_md5:
                continue
            seen_md5.add(md)
            unique_maps.append(m)

        missing_maps = [m for m in unique_maps if m.get("md5") not in existing_md5]
        total_missing = len(missing_maps)
        if total_missing == 0:
            messagebox.showinfo("Info", "All maps in the selected range are already present.")
            start_button.config(state=tk.NORMAL)
            status_label.config(text="Idle")
            return

        # confirm
        ok = messagebox.askyesno("Confirm download", f"{total_missing} maps missing.\nDownload now?")
        if not ok:
            start_button.config(state=tk.NORMAL)
            status_label.config(text="Cancelled")
            return

        # progress callback
        start_time = time.time()
        def progress_callback(done, total, start_time_inner):
            fraction = (done / total) if total else 1.0
            progress_bar["value"] = fraction * 100
            progress_bar.update()
            # ETA estimation
            elapsed = time.time() - start_time_inner
            avg = elapsed / done if done > 0 else 0
            remaining = int((total - done) * avg)
            mins, secs = divmod(remaining, 60)
            status_label.config(text=f"{done}/{total} maps | ETA: {mins}m {secs}s")
            count_label.config(text=f"{done}/{total}")

        # run download
        download_missing_maps(missing_maps, output_path, progress_callback)

        # extraction
        if auto_extract:
            status_label.config(text="Extracting...")
            extract_osz_files(output_path, delete_after=True)
            messagebox.showinfo("Done", "All downloads extracted and .osz files deleted.")
        else:
            status_label.config(text="Download complete.")
            ext = messagebox.askyesno("Extract now?", "Do you want to extract downloaded .osz files now?")
            if ext:
                extract_osz_files(output_path, delete_after=False)
                messagebox.showinfo("Done", "Extraction complete.")

        start_button.config(state=tk.NORMAL)
        status_label.config(text="Idle")

    threading.Thread(target=task, daemon=True).start()


# === GUI ===

def main_gui():
    root = tk.Tk()
    root.title("osumania table downloader for raja")
    root.geometry("600x520")
    root.resizable(False, False)

    ttk.Label(root, text="Select tables to download:").pack(pady=(12, 6))

    table_vars = {}
    level_min_vars = {}
    level_max_vars = {}

    def open_link(url):
        webbrowser.open(url)

    for table_name in ["7K + 8K", "4K"]:
        frame = ttk.Frame(root)
        frame.pack(pady=6, fill="x", padx=16)

        var = tk.BooleanVar(value=(table_name == "7K + 8K"))
        table_vars[table_name] = var

        cb = ttk.Checkbutton(frame, text=table_name, variable=var)
        cb.pack(side=tk.LEFT)

        link = ttk.Label(frame, text="table url", foreground="blue", cursor="hand2")
        link.pack(side=tk.LEFT, padx=8)
        link.bind("<Button-1>", lambda e, url=TABLE_URLS[table_name]: open_link(url))

        # integer spinboxes for min and max (0..20)
        ttk.Label(frame, text="Min:").pack(side=tk.LEFT, padx=(18,2))
        min_var = tk.IntVar(value=0)
        level_min_vars[table_name] = min_var
        spin_min = ttk.Spinbox(frame, from_=0, to=20, textvariable=min_var, width=4, justify="center")
        spin_min.pack(side=tk.LEFT, padx=(0,8))

        ttk.Label(frame, text="Max:").pack(side=tk.LEFT, padx=(6,2))
        max_var = tk.IntVar(value=13)
        level_max_vars[table_name] = max_var
        spin_max = ttk.Spinbox(frame, from_=0, to=20, textvariable=max_var, width=4, justify="center")
        spin_max.pack(side=tk.LEFT, padx=(0,6))

        # small note explaining halves are included automatically
        ttk.Label(frame, text="(Choose star rating range)").pack(side=tk.LEFT, padx=6)

    # auto extract checkbox
    auto_extract = tk.BooleanVar(value=True)
    ttk.Checkbutton(root, text="Automatically extract and delete .osz files after download", variable=auto_extract).pack(pady=10)

    # download location
    ttk.Label(root, text="Download location:").pack(pady=(8,2))
    output_path_var = tk.StringVar(value=os.path.join(os.getcwd(), "osudownloaderscript_downloads"))
    frame_out = ttk.Frame(root)
    frame_out.pack(pady=2)
    entry = ttk.Entry(frame_out, textvariable=output_path_var, width=46)
    entry.pack(side=tk.LEFT, padx=6)
    def browse_folder():
        folder = filedialog.askdirectory()
        if folder:
            output_path_var.set(folder)
    ttk.Button(frame_out, text="Browse", command=browse_folder).pack(side=tk.LEFT)

    # progress bar + detailed status
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=520, mode="determinate")
    progress_bar.pack(pady=(18,6))
    status_label = ttk.Label(root, text="Idle")
    status_label.pack()
    count_label = ttk.Label(root, text="0/0")
    count_label.pack()

    # start button
    def get_selected_tables_and_ranges():
        selected = [name for name, var in table_vars.items() if var.get()]
        ranges = {name: (level_min_vars[name].get(), level_max_vars[name].get()) for name in level_min_vars}
        return selected, ranges

    start_button = ttk.Button(root, text="Start Download",
                              command=lambda: start_download(
                                  *get_selected_tables_and_ranges(),
                                  auto_extract.get(),
                                  output_path_var.get(),
                                  progress_bar,
                                  status_label,
                                  start_button,
                                  count_label
                              ))
    start_button.pack(pady=14)

    ttk.Label(root, text="Make sure songdata.db is in the same directory.", foreground="gray").pack(pady=10)

    root.mainloop()


if __name__ == "__main__":
    main_gui()
