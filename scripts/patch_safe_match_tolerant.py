from pathlib import Path
p = Path(r"C:\Quant\scripts\regen_and_verify.py")
s = p.read_text(encoding="utf8")
old = "for idx, val in zip(left.index, m2[\"realized_return\"]):"
if old in s and "rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)" not in s:
    new = (
        "rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)\\n"
        "            if rr_col is None:\\n"
        "                raise KeyError(\\\"realized_return column missing in matched frame\\\")\\n"
        "            for idx, val in zip(left.index, m2[rr_col]):"
    )
    s = s.replace(old, new)
    p.write_text(s, encoding="utf8")
    print('patched safe_match in', p)
else:
    print('pattern not found or already patched')
