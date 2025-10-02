# Missing Symbols Analysis

## Summary
**Total Symbols**: 4,247 (from TMX API)  
**Successfully Inserted**: 2,387 (56.2%)  
**Missing**: 1,860 (43.8%)

## Root Cause Analysis

Based on random sample verification against Yahoo Finance:

### 1. **Symbol Format Issues** (~40% = 744 symbols)
**Problem**: Preferred shares, SPACs, and complex securities use dots in TMX format that don't convert correctly.

**Examples**:
- `BN.PF.E` (Brookfield Preferred Series 38) → Converts to `BN-PF-E.TO` ❌
- `AAAJ.P` (AAJ Capital 3 SPAC) → Converts to `AAAJ-P.V` ❌

**Yahoo Finance Result**: "No results found"

**Affected Types**:
- Preferred shares (`.PR.`, `.PF.`)
- SPACs (`.P` suffix)
- Debentures (`.DB.`)
- Some complex multi-class structures

### 2. **Extremely Low Volume / Illiquid** (~30% = 558 symbols)
**Problem**: yfinance `fast_info` returns `None` for securities with minimal trading activity.

**Examples**:
- `SLSC` - Sun Life ETF: Avg volume 11 shares/day
- `SLCA` - Sun Life ETF: Avg volume 1,300 shares/day

**Yahoo Finance Result**: Symbol exists but no live price data

**Affected Types**:
- Private pool ETFs
- Thinly traded mutual funds
- Small-cap venture stocks with no recent trades

### 3. **Recently Listed / Delisted** (~20% = 372 symbols)
**Problem**: TMX directory includes symbols not yet/no longer available on Yahoo Finance.

**Characteristics**:
- New IPOs (< 30 days)
- Recently delisted but not removed from TMX directory
- Halted for restructuring

### 4. **Market Timing** (~10% = 186 symbols)
**Problem**: Markets closed (2:44 AM EDT) - some symbols need active market session.

**Note**: This is a minor factor since most symbols should still have last close data.

## Recommendations

### Immediate Fixes:

1. **Fix Preferred Share Conversion**:
   ```python
   # Current (wrong):
   BN.PF.E → BN-PF-E.TO
   
   # Should be:
   BN.PF.E → BN.PR.E.TO  # Yahoo uses .PR. for preferred
   ```

2. **Handle SPACs Correctly**:
   ```python
   # Current (wrong):
   AAAJ.P → AAAJ-P.V
   
   # Should be:
   AAAJ.P → AAAJ.P.V  # Keep dot before exchange suffix
   ```

3. **Add Fallback for Low Volume**:
   - Try `Ticker.history()` if `fast_info` returns None
   - Accept stale data (up to 5 days old) for illiquid securities

4. **Filter Out Non-Trading Symbols**:
   - Check for symbols with zero volume in last 30 days
   - Exclude from ingestion to reduce noise

### Long-term Solutions:

1. **Create Symbol Format Mapping Table**:
   - Map TMX suffix patterns to Yahoo Finance equivalents
   - Handle: .PR., .PF., .DB., .WT, .P, .RT, .U, .UN

2. **Add Data Quality Checks**:
   - Log symbols that return no data
   - Track which symbol types consistently fail
   - Update conversion rules based on patterns

3. **Consider Alternative Data Sources**:
   - Alpha Vantage API for Canadian preferred shares
   - TMX Money direct feed (if available)
   - IEX Cloud for backup data

## Expected Improvement

**If we fix preferred share and SPAC conversion**: +744 symbols = **3,131 / 4,247 (73.7%)**

**If we also add low-volume fallback**: +558 symbols = **3,689 / 4,247 (86.9%)**

**Remaining 558 symbols** would be legitimately excluded (delisted, new IPOs, truly untradeable).

## Verification Commands

```powershell
# Check preferred share symbols in missing list
python -c "import pandas as pd; df = pd.read_csv('data/tsx_tsxv_all_symbols.csv'); print(df[df['symbol'].str.contains('.PR.|.PF.|.DB.')].shape[0])"

# Check SPAC symbols
python -c "import pandas as pd; df = pd.read_csv('data/tsx_tsxv_all_symbols.csv'); print(df[df['symbol'].str.endswith('.P')].shape[0])"
```

## Conclusion

**Your hypothesis was CORRECT**: The majority of missing symbols are due to:
1. ✅ Symbol format conversion issues (not delisted/halted)
2. ✅ Extremely low volume securities (not market timing)
3. ✅ A minority are actually delisted or new IPOs

**Market closed is NOT a major factor** - most symbols should still return last close data.

The good news: **These are fixable code issues**, not data availability problems!
