"""Truncate ALL `#[cfg(test)] mod tests` blocks at the LAST occurrence
of `#[cfg(test)]` at column 0 followed by `mod tests {` or `mod ... {`.

Also drops orphan `#[cfg(test)] use ...;` lines that precede the cut.
"""
import sys
import os

def truncate(path):
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
    cut = None
    for i, line in enumerate(lines):
        if line.rstrip() == '#[cfg(test)]':
            for j in range(i + 1, min(i + 3, len(lines))):
                nxt = lines[j].lstrip()
                if nxt.startswith('mod ') and '{' in nxt:
                    cut = i
                    break
    if cut is None:
        return None
    new_lines = lines[:cut]
    while len(new_lines) >= 2:
        last2 = new_lines[-2].rstrip()
        last1 = new_lines[-1].lstrip()
        if last2 == '#[cfg(test)]' and (last1.startswith('use ') or last1.startswith('extern ')):
            new_lines = new_lines[:-2]
            while new_lines and new_lines[-1].strip() == '':
                new_lines = new_lines[:-1]
            new_lines.append('\n')
        else:
            break
    if not new_lines or new_lines[-1].strip() != '':
        new_lines.append('\n')
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.writelines(new_lines)
    return (len(lines), len(new_lines))

count = 0
total_removed = 0
for root, dirs, files in os.walk('.'):
    dirs[:] = [d for d in dirs if d not in ('target', '.git', 'node_modules')]
    if '/src/' not in root.replace('\\', '/') + '/' and not root.replace('\\', '/').endswith('/src'):
        continue
    for fn in files:
        if not fn.endswith('.rs'):
            continue
        p = os.path.join(root, fn).replace('\\', '/')
        result = truncate(p)
        if result is not None:
            before, after = result
            removed = before - after
            total_removed += removed
            count += 1
            print(f'  CUT  {p}: {before} -> {after} (-{removed})')

print(f'\nTotal: {count} files truncated, {total_removed} lines removed')
