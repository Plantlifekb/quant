from pathlib import Path
p = Path(r"C:\Quant\scripts\regen_and_verify.py")
s = p.read_text(encoding="utf8")
lines = s.splitlines(True)
found = False
for i, line in enumerate(lines):
    if 'for idx, val in zip(left.index, m2[\"realized_return\"]):' in line:
        indent = line[:len(line) - len(line.lstrip())]
        new_block = (
            indent + 'rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)\\n'
            + indent + 'if rr_col is None:\\n'
            + indent + '    raise KeyError(\"realized_return column missing in matched frame\")\\n'
            + indent + 'for idx, val in zip(left.index, m2[rr_col]):\\n'
        )
        lines[i] = new_block
        found = True
        break
if not found:
    print('pattern not found; no changes made')
else:
    p.write_text(''.join(lines), encoding='utf8')
    print('patched regen_and_verify.py')
