"""
serve.py  —  Start local dashboard server
Run:  python serve.py
Open: http://localhost:5050
"""
import http.server, socketserver, os, sys, webbrowser, threading, time

PORT = 5050
os.chdir(os.path.dirname(os.path.abspath(__file__)))

if not os.path.exists("data.json"):
    print("\n❌  data.json not found. Run first:\n    python prepare_data.py\n")
    sys.exit(1)

class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args): pass  # suppress request logs

def open_browser():
    time.sleep(0.8)
    webbrowser.open(f"http://localhost:{PORT}")

print(f"\n🚀  http://localhost:{PORT}  |  Ctrl+C to stop\n")
threading.Thread(target=open_browser, daemon=True).start()
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    try: httpd.serve_forever()
    except KeyboardInterrupt: print("\n👋  Server stopped.")
