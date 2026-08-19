import test from 'node:test';
import assert from 'node:assert/strict';
import { constantTimeEqual, createSessionToken, decryptJson, encryptJson, randomPairingCode, verifySessionToken } from '../src/security.js';

const key=Buffer.alloc(32,7).toString('base64');
test('AES-GCM credential envelope round trips without plaintext',async()=>{const value={swid:'abc',s2:'very-secret-cookie'};const envelope=await encryptJson(value,key);assert.equal(envelope.includes(value.s2),false);assert.deepEqual(await decryptJson(envelope,key),value)});
test('tampered credential envelope fails closed',async()=>{const envelope=JSON.parse(await encryptJson({s2:'secret'},key));envelope.ciphertext=envelope.ciphertext.slice(0,-2)+'AA';await assert.rejects(()=>decryptJson(JSON.stringify(envelope),key))});
test('signed sessions verify and tampering fails',async()=>{const secret='x'.repeat(40),token=await createSessionToken(secret,60);assert.ok(await verifySessionToken(token,secret));assert.equal(await verifySessionToken(`${token}x`,secret),null)});
test('constant-time comparator returns expected equality',async()=>{assert.equal(await constantTimeEqual('same','same'),true);assert.equal(await constantTimeEqual('same','different'),false)});
test('pairing codes omit ambiguous characters',()=>{const code=randomPairingCode(100);assert.equal(/[01IO]/.test(code),false)});
