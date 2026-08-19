#!/usr/bin/env python3
"""Small zero-dependency secret scanner for CI and pre-commit checks."""
from __future__ import annotations
import re
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SKIP={'.git','node_modules','.wrangler','pipeline/output','coverage'}
TEXT_SUFFIXES={'.js','.json','.jsonc','.md','.py','.sql','.yml','.yaml','.toml','.txt','.html','.css','.svg','.example',''}
PATTERNS={
 'ESPN session cookie':re.compile(r'(?i)(?:espn_s2|SWID)\s*[=:]\s*["\']?(?!\$\{|\[REDACTED\]|replace|example)[A-Za-z0-9{}._%\-]{20,}'),
 'private key':re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
 'GitHub token':re.compile(r'\b(?:ghp|github_pat)_[A-Za-z0-9_]{30,}\b'),
 'Cloudflare token assignment':re.compile(r'(?i)CLOUDFLARE_API_TOKEN\s*[=:]\s*["\']?(?!\$\{|replace)[A-Za-z0-9_\-]{20,}'),
 'generic bearer token':re.compile(r'(?i)Authorization["\']?\s*:\s*["\']Bearer\s+(?!\$\{|\$|<|replace)[A-Za-z0-9._\-]{24,}'),
}

def files():
 for path in ROOT.rglob('*'):
  if not path.is_file(): continue
  relative=path.relative_to(ROOT)
  if any(str(relative).startswith(prefix) for prefix in SKIP): continue
  if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {'.gitignore','.editorconfig','.env.example'}: continue
  yield path

def main()->int:
 findings=[]
 for path in files():
  try: text=path.read_text(encoding='utf-8')
  except UnicodeDecodeError: continue
  for line_number,line in enumerate(text.splitlines(),1):
   for label,pattern in PATTERNS.items():
    if pattern.search(line): findings.append(f'{path.relative_to(ROOT)}:{line_number}: {label}')
 if findings:
  print('Potential secrets found:',file=sys.stderr)
  print('\n'.join(findings),file=sys.stderr)
  return 1
 print('Secret scan passed.')
 return 0
if __name__=='__main__': raise SystemExit(main())
