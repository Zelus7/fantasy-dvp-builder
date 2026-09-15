import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const apiSource=source.split('\n').find(line=>line.startsWith('async function api('));
for(const [code,expectedLogins] of [['AUTH_REQUIRED',1],['ESPN_AUTH_EXPIRED',0],['ESPN_UPSTREAM_ERROR',0]]){
  test(`API error ${code} redirects only for an expired app session`,async()=>{
    let logins=0;
    const context=vm.createContext({fetch:async()=>({ok:false,status:401,json:async()=>({error:{code,message:'Test error'}})}),showLogin(){logins++}});
    vm.runInContext(apiSource,context);
    await assert.rejects(context.api('/api/dashboard'),{code});
    assert.equal(logins,expectedLogins);
  });
}
