import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { corsHeaders } from '../src/http.js';

test('extension CORS reflects extension origins only',()=>{
  const extension=corsHeaders(new Request('https://app.example/api/extension/pair',{headers:{Origin:'chrome-extension://abcdefghijklmnop'}}));
  const web=corsHeaders(new Request('https://app.example/api/extension/pair',{headers:{Origin:'https://attacker.example'}}));
  assert.equal(extension['Access-Control-Allow-Origin'],'chrome-extension://abcdefghijklmnop');
  assert.equal('Access-Control-Allow-Origin' in web,false);
});

test('HTML shell does not ship JavaScript template syntax as text',()=>{
  const html=fs.readFileSync(new URL('../public/index.html',import.meta.url),'utf8');
  assert.doesNotMatch(html,/\$\{\['dashboard'/);
  assert.match(html,/<main><\/main>/);
});
