import pandas as pd

df = pd.read_csv('data/tsx_tsxv_all_symbols.csv')

print("Sample preferred shares (.PF.):")
pref = df[df['symbol'].fillna('').str.contains('\\.PF\\.')].head(5)[['symbol', 'name', 'exchange']]
print(pref)

print("\nSample SPACs (.P):")
spacs = df[df['symbol'].fillna('').str.endswith('.P')].head(5)[['symbol', 'name', 'exchange']]
print(spacs)

print("\nSample multi-class (with dots):")
multi = df[(df['symbol'].fillna('').str.contains('\\.')) & 
           (~df['symbol'].fillna('').str.contains('\\.P[^R]')) & 
           (~df['symbol'].fillna('').str.contains('\\.PF\\.'))].head(10)[['symbol', 'name', 'exchange']]
print(multi)

print("\n\nNow test the conversion for these symbols:")
def tmx_to_yahoo(tmx_sym, exchange):
    """Test conversion function."""
    if '.PF.' in tmx_sym:
        yahoo_base = tmx_sym.replace('.PF.', '.PR.')
    elif tmx_sym.endswith('.P'):
        yahoo_base = tmx_sym
    elif '.DB.' in tmx_sym:
        yahoo_base = tmx_sym
    elif tmx_sym.endswith('.WT'):
        yahoo_base = tmx_sym
    elif tmx_sym.endswith('.RT'):
        yahoo_base = tmx_sym
    else:
        yahoo_base = tmx_sym.replace('.', '-')
    
    if exchange == 'TSX':
        return f"{yahoo_base}.TO"
    elif exchange == 'TSXV':
        return f"{yahoo_base}.V"
    else:
        return f"{yahoo_base}.TO"

test_symbols = [
    ('ACO.X', 'TSX'),
    ('BN.PF.E', 'TSX'),
    ('AAAJ.P', 'TSXV'),
    ('TD.PR.A', 'TSX'),
    ('BBD.A', 'TSX'),
]

for sym, exch in test_symbols:
    yahoo = tmx_to_yahoo(sym, exch)
    print(f"{sym:15} ({exch:4}) → {yahoo}")
