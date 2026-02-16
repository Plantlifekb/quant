from pathlib import Path
p = Path(r"C:\Quant\scripts\regen_and_verify.py")
s = p.read_text(encoding="utf8")

old = (
    "rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)\\n"
    "            if rr_col is None:\\n"
    "                raise KeyError(\"realized_return column missing in matched frame\")\\n"
    "            for idx, val in zip(left.index, m2[rr_col]):"
)

if old in s:
    new = (
        "rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)\n"
        "            if rr_col is None:\n"
        "                raise KeyError(\"realized_return column missing in matched frame\")\n"
        "            for idx, val in zip(left.index, m2[rr_col]):"
    )
    s = s.replace(old, new)
    p.write_text(s, encoding="utf8")
    print('patched regen_and_verify.py')
else:
    # show nearby context to help diagnose why pattern not found
    lines = s.splitlines()
    for i, line in enumerate(lines):
        if 'realized_return' in line:
            start = max(0, i-4)
            end = min(len(lines), i+4)
            print('pattern not found; showing context lines', start+1, 'to', end)
            print('\\n'.join(lines[start:end]))
            break
