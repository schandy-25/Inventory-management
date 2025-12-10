import sqlite3, os, sys
DB = sys.argv[1] if len(sys.argv) > 1 else "inventory.db"
out = sys.argv[2] if len(sys.argv) > 2 else "er_diagram.mmd"
if not os.path.exists(DB): raise SystemExit(f"DB not found: {DB}")
conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
tables = [r[0] for r in cur.fetchall()]
def cols(t):
    cur.execute(f"PRAGMA table_info('{t}')"); return cur.fetchall()  # cid,name,type,notnull,dflt,pk
def fks(t):
    cur.execute(f"PRAGMA foreign_key_list('{t}')"); return cur.fetchall()  # id,seq,table,from,to,...
def norm(t): return (t or "text").split()[0].lower()
lines = ["erDiagram"]
for t in tables:
    lines.append(f"  {t.upper()} {{")
    for _, name, ctype, _nn, _dflt, pk in cols(t):
        lines.append(f"    {norm(ctype)} {name}{' PK' if pk else ''}")
    lines.append("  }")
for t in tables:
    for (_id,_seq,parent,col_from,col_to,*_) in fks(t):
        lines.append(f"  {parent.upper()} ||--o{{ {t.upper()} : references")
with open(out,"w") as f: f.write("\n".join(lines))
print(f"Wrote Mermaid ER to {out}")
