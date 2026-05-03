import os
import sys
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

private_key = os.getenv("POLY_PRIVATE_KEY", "")
if not private_key.startswith("0x"):
    private_key = "0x" + private_key
funder = os.getenv("POLY_FUNDER_ADDRESS", "")

# Step 1: Derive the EOA address from the private key
print("=" * 60)
print("STEP 1: Verifying private key -> address mapping")
print("=" * 60)

try:
    from eth_account import Account
    acct = Account.from_key(private_key)
    print(f"  Private key resolves to address: {acct.address}")
    print(f"  Funder address from .env:        {funder}")
    if funder and acct.address.lower() == funder.lower():
        print("  >> These are the SAME address (plain EOA, no proxy)")
    else:
        print("  >> These are DIFFERENT (expected for MetaMask + Polymarket proxy)")
except ImportError:
    print("  eth_account not installed, skipping address check")
except Exception as e:
    print(f"  Error: {e}")

# Step 2: Try py_clob_client methods
print()
print("=" * 60)
print("STEP 2: Trying credential generation")
print("=" * 60)

from py_clob_client.client import ClobClient

# Build all combos to try
combos = []
if funder:
    combos.append(("sig=0 + funder", dict(key=private_key, chain_id=137, signature_type=0, funder=funder)))
    combos.append(("sig=1 + funder", dict(key=private_key, chain_id=137, signature_type=1, funder=funder)))
    combos.append(("sig=2 + funder", dict(key=private_key, chain_id=137, signature_type=2, funder=funder)))
combos.append(("sig=0 no funder",    dict(key=private_key, chain_id=137, signature_type=0)))

for label, kwargs in combos:
    print(f"\n  Trying: {label}")
    try:
        client = ClobClient(host="https://clob.polymarket.com", **kwargs)
        
        # Try derive first (for existing keys)
        try:
            creds = client.derive_api_key()
            print(f"    derive_api_key() WORKED!")
            print(f"    POLY_API_KEY={creds.api_key}")
            print(f"    POLY_API_SECRET={creds.api_secret}")
            print(f"    POLY_API_PASSPHRASE={creds.api_passphrase}")
            print("\n    >> Copy these into your .env file!")
            sys.exit(0)
        except Exception as e1:
            print(f"    derive_api_key() failed: {e1}")
        
        # Try create
        try:
            creds = client.create_api_key()
            print(f"    create_api_key() WORKED!")
            print(f"    POLY_API_KEY={creds.api_key}")
            print(f"    POLY_API_SECRET={creds.api_secret}")
            print(f"    POLY_API_PASSPHRASE={creds.api_passphrase}")
            print("\n    >> Copy these into your .env file!")
            sys.exit(0)
        except Exception as e2:
            print(f"    create_api_key() failed: {e2}")

    except Exception as e:
        print(f"    Client init failed: {e}")

# Step 3: Try the combo method as last resort
print()
print("=" * 60)
print("STEP 3: Trying create_or_derive with nonce=1")
print("=" * 60)

for nonce in [0, 1, 2]:
    for sig_type in [0, 1, 2]:
        funder_arg = funder if funder else None
        label = f"nonce={nonce}, sig={sig_type}, funder={'yes' if funder_arg else 'no'}"
        try:
            kwargs = dict(key=private_key, chain_id=137, signature_type=sig_type)
            if funder_arg:
                kwargs["funder"] = funder_arg
            client = ClobClient(host="https://clob.polymarket.com", **kwargs)
            creds = client.create_or_derive_api_creds(nonce=nonce)
            print(f"  {label} -> SUCCESS!")
            print(f"    POLY_API_KEY={creds.api_key}")
            print(f"    POLY_API_SECRET={creds.api_secret}")
            print(f"    POLY_API_PASSPHRASE={creds.api_passphrase}")
            print("\n    >> Copy these into your .env file!")
            sys.exit(0)
        except Exception as e:
            pass  # silently skip failures

print()
print("[FAILED] All combinations exhausted.")
print("The private key in .env does not match any registered Polymarket account.")
