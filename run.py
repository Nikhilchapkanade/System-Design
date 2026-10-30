#!/usr/bin/env python3
"""
System Design Project Runner & Manager
Interactive CLI to run, test, and benchmark any of the system design projects.
"""

import os
import sys
import subprocess

PROJECTS = [
    {
        "id": "01",
        "name": "URL Shortener (TinyURL)",
        "folder": "01-url-shortener",
        "port": 8001,
        "entry": "app.main:app",
        "type": "fastapi",
    },
    {
        "id": "02",
        "name": "API Rate Limiter",
        "folder": "02-rate-limiter",
        "port": 8002,
        "entry": "app.main:app",
        "type": "fastapi",
    },
    {
        "id": "03",
        "name": "LSM-Tree Key-Value Store",
        "folder": "03-key-value-store",
        "port": 8003,
        "entry": "cli.py",
        "type": "cli",
    },
    {
        "id": "04",
        "name": "Real-Time Chat Engine",
        "folder": "04-realtime-chat",
        "port": 8004,
        "entry": "app.main:app",
        "type": "fastapi",
    },
    {
        "id": "05",
        "name": "Layer-7 Load Balancer",
        "folder": "05-load-balancer",
        "port": 8005,
        "entry": "app.main:app",
        "type": "fastapi",
    },
    {
        "id": "06",
        "name": "Distributed Task Queue",
        "folder": "06-distributed-task-queue",
        "port": 8006,
        "entry": "app.main:app",
        "type": "fastapi",
    },
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_python_exe(folder):
    """Find the virtualenv python executable for a given project."""
    win_path = os.path.join(BASE_DIR, folder, ".venv", "Scripts", "python.exe")
    posix_path = os.path.join(BASE_DIR, folder, ".venv", "bin", "python")
    if os.path.exists(win_path):
        return win_path
    if os.path.exists(posix_path):
        return posix_path
    return sys.executable

def run_project(proj):
    python_exe = get_python_exe(proj["folder"])
    proj_dir = os.path.join(BASE_DIR, proj["folder"])
    print(f"\n[+] Launching {proj['name']}...")
    if proj["type"] == "fastapi":
        print(f"[*] Dashboard URL: http://127.0.0.1:{proj['port']}")
        cmd = [python_exe, "-m", "uvicorn", proj["entry"], "--host", "127.0.0.1", "--port", str(proj["port"]), "--reload"]
    else:
        cmd = [python_exe, proj["entry"]]
    
    subprocess.run(cmd, cwd=proj_dir)

def run_tests(proj):
    python_exe = get_python_exe(proj["folder"])
    proj_dir = os.path.join(BASE_DIR, proj["folder"])
    print(f"\n[+] Running test suite for {proj['name']}...")
    cmd = [python_exe, "-m", "pytest", "tests", "-v"]
    subprocess.run(cmd, cwd=proj_dir)

def run_benchmark(proj):
    python_exe = get_python_exe(proj["folder"])
    proj_dir = os.path.join(BASE_DIR, proj["folder"])
    benchmark_file = os.path.join(proj_dir, "benchmark.py")
    if not os.path.exists(benchmark_file):
        print(f"[-] No benchmark.py found in {proj['folder']}")
        return
    print(f"\n[+] Running benchmark for {proj['name']}...")
    cmd = [python_exe, "benchmark.py"]
    subprocess.run(cmd, cwd=proj_dir)

def main():
    while True:
        print("\n" + "=" * 60)
        print("        SYSTEM DESIGN PORTFOLIO MANAGER")
        print("=" * 60)
        for p in PROJECTS:
            print(f"  [{p['id']}] {p['name']} (Port: {p['port']})")
        print("  [T] Run ALL test suites across all 6 projects")
        print("  [B] Run ALL benchmarks across all 6 projects")
        print("  [Q] Exit")
        print("-" * 60)
        
        choice = input("Select an option (01-06, T, B, Q): ").strip().upper()
        
        if choice == "Q":
            print("Goodbye!")
            break
        elif choice == "T":
            for p in PROJECTS:
                run_tests(p)
        elif choice == "B":
            for p in PROJECTS:
                run_benchmark(p)
        else:
            match = next((p for p in PROJECTS if p["id"] == choice or choice == str(int(p["id"]) if p["id"].isdigit() else "")), None)
            if not match:
                print("Invalid selection, please try again.")
                continue
            
            print(f"\nActions for {match['name']}:")
            print("  1. Run Server / CLI")
            print("  2. Run Tests")
            print("  3. Run Benchmark")
            print("  0. Back to Main Menu")
            sub_choice = input("Select action (1-3, 0): ").strip()
            
            if sub_choice == "1":
                run_project(match)
            elif sub_choice == "2":
                run_tests(match)
            elif sub_choice == "3":
                run_benchmark(match)

if __name__ == "__main__":
    main()
