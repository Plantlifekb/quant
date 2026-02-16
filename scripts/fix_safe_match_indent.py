from pathlib import Path
p = Path(r"C:\Quant\scripts\regen_and_verify.py")
s = p.read_text(encoding="utf8")
lines = s.splitlines(True)

# Locate the broken area by searching for the literal escaped-newline pattern or the broken rr_col line
start_idx = None
for i, L in enumerate(lines):
    if "\\\\n" in L and "realized_return" in L:
        start_idx = i
        break
    if "for idx, val in zip(left.index, m2[\"realized_return\"]):" in L:
        start_idx = i
        break
# If not found, try to find any nearby occurrence of realized_return in the file
if start_idx is None:
    for i, L in enumerate(lines):
        if "realized_return" in L and "safe_match" in ''.join(lines[max(0,i-20):i+20]):
            start_idx = i
            break

if start_idx is None:
    print("could_not_find_target_block")
else:
    # Walk backwards to find the start of the safe_match loop block (look for the function or the left/m2 usage)
    block_start = start_idx
    while block_start > 0 and not lines[block_start].lstrip().startswith("def "):
        if "safe_match" in lines[block_start]:
            break
        block_start -= 1
    # Find the line index where the original for-loop should be replaced
    # We'll search forward from start_idx for the first line containing "for idx, val" or "m2[rr_col]"
    block_end = start_idx
    for j in range(start_idx, min(len(lines), start_idx+20)):
        if "for idx, val" in lines[j] or "m2[rr_col]" in lines[j]:
            block_end = j
            break

    # Determine indentation level from the line where the original for-loop was found (or use 12 spaces)
    indent_line = lines[block_end] if block_end < len(lines) else lines[start_idx]
    indent = indent_line[:len(indent_line) - len(indent_line.lstrip())]

    # Construct the replacement block (preserve indent)
    new_block = (
        indent + 'rr_col = next((c for c in ["realized_return","realized","return","ret"] if c in m2.columns), None)\n'
        + indent + 'if rr_col is None:\n'
        + indent + '    raise KeyError("realized_return column missing in matched frame")\n'
        + indent + 'for idx, val in zip(left.index, m2[rr_col]):\n'
    )

    # Replace the single line or small chunk with the new block
    # Replace from block_end to block_end (single-line replacement) if that line contains the old for-loop,
    # otherwise replace a small window around start_idx..block_end
    replace_from = block_end
    replace_to = block_end + 1
    # If the found line is the literal escaped block, expand the window a bit
    if "\\\\n" in lines[start_idx]:
        replace_from = max(0, start_idx - 1)
        replace_to = min(len(lines), start_idx + 4)

    lines[replace_from:replace_to] = [new_block]
    p.write_text(''.join(lines), encoding='utf8')
    print("patched_regen_and_verify_safe_match")
