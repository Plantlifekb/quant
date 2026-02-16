from pathlib import Path
p = Path(r"C:\Quant\scripts\regen_and_verify.py")
s = p.read_text(encoding="utf8")

# Find the start of the broken insertion by locating the rr_col literal with backslashes
start_token = "rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)\\n"
if start_token in s:
    # Replace the literal-escaped block with a real multi-line block, preserving indentation
    s = s.replace(
        start_token
        + "            if rr_col is None:\\n"
        + "                raise KeyError(\\\"realized_return column missing in matched frame\\\")\\n"
        + "            for idx, val in zip(left.index, m2[rr_col]):",
        (
            "rr_col = next((c for c in [\"realized_return\",\"realized\",\"return\",\"ret\"] if c in m2.columns), None)\n"
            "            if rr_col is None:\n"
            "                raise KeyError(\"realized_return column missing in matched frame\")\n"
            "            for idx, val in zip(left.index, m2[rr_col]):"
        )
    )
    p.write_text(s, encoding="utf8")
    print('patched regen_and_verify.py (replaced escaped-newline block)')
else:
    # Fallback: try to locate any line containing the literal backslash-n pattern and fix around it
    lines = s.splitlines(True)
    changed = False
    for i, line in enumerate(lines):
        if "\\\\n" in line and "realized_return" in line:
            # find the contiguous chunk that contains backslash-n sequences
            j = i
            chunk = []
            while j < len(lines) and ("\\n" in lines[j] or "realized_return" in lines[j] or "m2[rr_col]" in lines[j] or "rr_col" in lines[j]):
                chunk.append(lines[j])
                j += 1
            indent = lines[i][:len(lines[i]) - len(lines[i].lstrip())]
            new_block = (
                indent + 'rr_col = next((c for c in ["realized_return","realized","return","ret"] if c in m2.columns), None)\n'
                + indent + 'if rr_col is None:\n'
                + indent + '    raise KeyError("realized_return column missing in matched frame")\n'
                + indent + 'for idx, val in zip(left.index, m2[rr_col]):\n'
            )
            lines[i:j] = [new_block]
            changed = True
            break
    if changed:
        p.write_text(''.join(lines), encoding="utf8")
        print('patched regen_and_verify.py (fallback replacement)')
    else:
        print('no matching escaped block found; no changes made')
