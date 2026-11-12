import hashlib, hmac, secrets, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SALT, HASH = open('/auth/.authhash').read().strip().split(':')
SAIOAK = {}            # token -> iraungitze-ordua
IRAUPENA = 8 * 3600    # gehienez 8 ordu

ORRIA = """<!doctype html><html lang="eu"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>CNC Guard - Kibana</title>
<style>body{font-family:Arial,sans-serif;background:#f1f4f8;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
form{background:#fff;padding:32px;border-radius:8px;box-shadow:0 2px 12px #0002;width:280px}
h1{font-size:20px;margin:0 0 16px}input,button{width:100%;padding:10px;margin-top:8px;box-sizing:border-box;font-size:15px}
button{background:#0b64dd;color:#fff;border:0;border-radius:4px;cursor:pointer}p{color:#c00;min-height:18px}</style></head>
<body><form method="post" action="/__auth/login" onsubmit="sessionStorage.setItem('cncGate','1')">
<h1>CNC Guard - Kibana</h1><input type="password" name="password" placeholder="Pasahitza" autofocus required>
<button type="submit">Sartu</button><p>{ERR}</p></form></body></html>"""

class H(BaseHTTPRequestHandler):
    def cookie(self):
        for zatia in self.headers.get('Cookie', '').split(';'):
            k, _, v = zatia.strip().partition('=')
            if k == 'cncsess':
                return v
    def ondo(self):
        t = self.cookie()
        return bool(t) and SAIOAK.get(t, 0) > time.time()
    def erantzun(self, kodea, goiburuak=(), gorputza=b''):
        self.send_response(kodea)
        for k, v in goiburuak:
            self.send_header(k, v)
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(gorputza)
    def kudeatu(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == '/check':
            return self.erantzun(200 if self.ondo() else 401)
        if u.path == '/__auth/logout':
            SAIOAK.pop(self.cookie(), None)
            return self.erantzun(302, [('Set-Cookie', 'cncsess=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax'), ('Location', '/__auth/login')])
        if u.path == '/__auth/login' and self.command == 'GET':
            err = 'Pasahitz okerra' if 'error=1' in u.query else ''
            return self.erantzun(200, [('Content-Type', 'text/html; charset=utf-8')], ORRIA.replace('{ERR}', err).encode())
        if u.path == '/__auth/login' and self.command == 'POST':
            n = int(self.headers.get('Content-Length', 0))
            pw = urllib.parse.parse_qs(self.rfile.read(n).decode()).get('password', [''])[0]
            if hmac.compare_digest(hashlib.sha256((SALT + pw).encode()).hexdigest(), HASH):
                t = secrets.token_urlsafe(32)
                SAIOAK[t] = time.time() + IRAUPENA
                return self.erantzun(302, [('Set-Cookie', f'cncsess={t}; Path=/; HttpOnly; SameSite=Lax'), ('Location', '/app/dashboards')])
            time.sleep(1)
            return self.erantzun(302, [('Location', '/__auth/login?error=1')])
        self.erantzun(404)
    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = kudeatu
    def log_message(self, *a):
        pass

ThreadingHTTPServer(('0.0.0.0', 8080), H).serve_forever()