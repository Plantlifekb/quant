import re, pathlib, sys
p = pathlib.Path("src/quant/engine/stages")
for f in p.glob("*.py"):
    s = f.read_text(encoding="utf-8")
    # find conn.execute(text(""" ... """)) with extra ) and collapse them to exactly three closing parens
    s2 = re.sub(r'(conn\.execute\s*\(\s*text\s*\(\s*""".*?""")\)+', r"\1))", s, flags=re.S)
    if s2 != s:
        f.write_text(s2, encoding="utf-8")
        print("Patched", f)
print("Done")
